# Raft clusters (replicated transaction servers)

This guide is for operators who want to run `fluree-server` as a Raft-replicated cluster instead of a single-node transaction server. In this mode every node accepts client traffic, but writes are committed through a Raft log so the cluster survives the loss of any minority of nodes.

Coverage:

- [When to use Raft mode](#when-to-use-raft-mode)
- [Architecture](#architecture)
- [Per-node configuration](#per-node-configuration)
- [Bootstrapping a 3-node cluster](#bootstrapping-a-3-node-cluster)
- [Day-2 operations](#day-2-operations)
- [Admin HTTP surface](#admin-http-surface)
- [Client traffic and follower forwarding](#client-traffic-and-follower-forwarding)
- [Failure modes](#failure-modes)
- [Operational constraints and limits](#operational-constraints-and-limits)
- [Security notes](#security-notes)

For the deeper architecture (log entry types, snapshot model, queue/worker design), see the design doc: [Raft command queue and replicated state machine](../design/raft-command-queue.md).

## When to use Raft mode

Use it when:

- you need high availability for writes (single-node loss must not stop transactions),
- you can put all nodes behind one shared, content-addressed storage backend (typically S3 with a shared DynamoDB nameservice in the cloud profile, or a shared file mount in a homogeneous cluster),
- you can give each node a stable identity (node id) and a stable VPC-internal address for inter-node RPC.

Do **not** use Raft mode for:

- single-node deployments — the overhead of running a 1-node cluster is needless complexity. Use the default `transaction` server role instead.
- query-only replicas — `--server-role peer` already solves that, with lighter operational overhead.
- proxy-storage peers — Raft mode replicates writes through the log, which is the opposite of proxy mode (which forwards writes to a remote transaction server). Validation rejects the combination at startup.

## Architecture

A Fluree Raft cluster is a replicated state machine over an [openraft](https://github.com/databendlabs/openraft) log. Every node runs the same state machine; the leader is the only node that proposes new log entries. The state machine's job is to replicate **nameservice state** — branch heads, ledger lifecycle, and a deduplicating idempotency cache — not the commit blobs themselves. Commit blobs live in the shared content-addressed storage backend (CAS) and the log carries content identifiers, not payloads.

This split is the central architectural choice: the Raft log stays narrow (CIDs, branch ids, queue positions) while the heavy bytes (commit envelopes, index artifacts) ride the CAS that every node already needs to reach. A follower that has applied a log entry can serve reads of the new head as soon as the relevant blob arrives from CAS — without an openraft RPC.

### Two-listener topology

Each node listens on **two ports**:

| Port                                              | Default     | Faces        | Routes                                                | Auth                                                              |
| ------------------------------------------------- | ----------- | ------------ | ----------------------------------------------------- | ----------------------------------------------------------------- |
| Client (`--listen-addr`)                          | `0.0.0.0:8080` | Public / load balancer | `/fluree/*`, `/api/*`, `/admin/*`, etc.                | Same as single-node: `--events-auth-*`, `--data-auth-*`, `--admin-auth-*` |
| Raft (`--raft-listen-addr`)                       | (no default; required) | VPC-internal | `/raft/*` (inter-node RPC), `/cluster/*` (admin) | `/raft/*`: none (network trust). `/cluster/*`: `--admin-auth-mode` |

The client port is the public surface and behaves identically to a non-Raft `fluree-server` — reads work on every node, writes are accepted but transparently forwarded to the current leader by middleware. The Raft port is for inter-node openraft traffic plus operator-facing cluster admin; expose it only on a trusted network segment.

### Submission flow (writes)

Writes follow a four-stage path inside the cluster. The first two stages run wherever the request lands (follower or leader); the latter two run only on the leader:

1. **Forward.** The follower-forward middleware reads `raft.current_leader()`; if this node is not the leader, the request is re-issued as an HTTP call against the leader's `client_addr` (looked up from the membership the cluster replicates) and the leader's response is relayed back to the client verbatim.
2. **Enqueue.** On the leader, the `QueuedTransactor` builds a `QueuedRequest` envelope (the full request body), writes it to the shared CAS, and proposes a `Command::EnqueueCommand` carrying the envelope's CID + body hash. The state machine appends a `QueueEntry` to the target branch's FIFO and assigns a `queue_id`. The transactor registers a oneshot waiter on the per-process `WaiterMap`.
3. **Stage and commit.** The leader-only `CommitWorker` polls per-branch queues, fetches the envelope from CAS, stages the work via the in-process `Fluree` API, writes the resulting commit blob to CAS, stashes a typed `AppliedReceipt` in the per-process `StagedReceiptMap`, and proposes `Command::ApplyHead` to advance the head.
4. **Apply and resolve.** The state-machine adapter applies `ApplyHead` on every node (advancing the replicated head), and on the leader signals the waiter with the stashed receipt. The transactor's `await` returns the typed receipt to the client.

The reason for the queue + worker split is that **only the leader can propose log entries**, but the heavy work of staging a transaction (parsing, policy evaluation, indexing) must happen *before* the head moves. Decoupling proposal from staging keeps the openraft commit path free of blocking work, makes idempotent retries cheap (the second `EnqueueCommand` with the same idempotency key hits the cache and skips staging), and lets a leader change abandon in-flight stages without losing the queue.

### Storage layering

| Layer                          | Scope            | Backed by                                                                | Notes                                                                                                                                              |
| ------------------------------ | ---------------- | ------------------------------------------------------------------------ | -------------------------------------------------------------------------------------------------------------------------------------------------- |
| Raft log + vote + snapshots    | Per node         | Local disk at `--raft-storage-path`                                       | Postcard-encoded, atomic writes (write-to-temp → fsync → rename → fsync parent). Losing this directory on one node is recoverable as long as a quorum survives. |
| Replicated state machine       | Per node, in-memory | `NameServiceState` (branch heads, ledger registry, idempotency cache, per-branch queues) | Persisted via openraft snapshots into `--raft-storage-path/snapshots/`. Restored on restart so log replay starts at `last_applied + 1`. |
| Ledger CAS (commit blobs, envelopes, index artifacts) | Cluster-wide | Whatever `--connection-config` / `--storage-path` resolves to (S3, file, memory) | Must be reachable from every node. The leader writes, every node reads. |

`--raft-storage-path` and `--storage-path` must point at **disjoint** filesystem subtrees. Validation rejects overlapping paths at startup because the Raft log/snapshot tree and the ledger CAS each manage their own layout; overlapping them lets either side blow away the other's files on compaction and tends to surface only after a restart corrupts state.

### Replicated nameservice

`RaftNameService` implements the same `NameServiceLookup` / `CommitPublisher` / `IndexPublisher` traits as the standalone nameservice, but its writes propose log entries and its reads observe the state machine the adapter writes to under apply. Followers see committed state immediately (no openraft RPC on the read path); only `current_leader()` is queried from openraft for the forwarding decision.

Three publish operations cross the log:

- `CommitPublisher::publish_commit` → `Command::ApplyHead { queue_id, head, ... }` — strict head advance, keyed by the queue position the worker is draining.
- `IndexPublisher::publish_index` → `Command::AdvanceIndexHead` — strict monotonic index pointer advance.
- Admin rebinds (`bind-ref` / `set-head`) → `Command::RewriteIndexHead` / `Command::CompareAndSetRef` — allow equal-or-different, gated by an expected-prior CID for CAS semantics.

### Idempotency cache

The state machine carries a hash map from `IdempotencyCacheKey` (ledger id + caller-supplied key) to `ApplyOutcome` (either `Applied { head, t, body_cid, ... }` or `Failed { reason, ... }`). On a re-proposal with the same key the state machine short-circuits to `IdempotencyHit` / `IdempotencyFailed` without re-staging. Entries age out via a leader-only `EvictIdempotency` scheduler (default TTL **1 hour**, eviction tick **60 s**). After the TTL elapses, a retry with the same key looks like a fresh submission and may execute again — clients that need stricter at-most-once semantics must use shorter retry windows.

### Snapshots

openraft drives snapshotting on a configured policy (log-entries-since-last-snapshot). The state-machine adapter serializes the full `NameServiceState` via postcard, plus `last_applied` and `last_membership`. Snapshots are stored as `<id>.meta` + `<id>.data` under `--raft-storage-path/raft/snapshots/`, with a `current` file naming the latest. On restart the adapter opens the latest snapshot first so log replay can skip everything before `last_applied`.

Snapshot ids arriving from peers are validated before any disk write: non-empty, ≤ 128 bytes, alphanumeric + `-_.` only, no `..`. Anything else is rejected as `StorageError::Corruption`.

## Per-node configuration

Raft mode is built behind a `raft` Cargo feature. A build without the feature has no `--raft-*` flags at all, so single-node binaries don't pull in openraft.

When the feature is enabled, four CLI/env switches govern Raft mode:

| CLI flag                | Env var                        | Required when raft is on |
| ----------------------- | ------------------------------ | ------------------------ |
| `--raft-enabled`        | `FLUREE_RAFT_ENABLED`          | toggle                   |
| `--raft-node-id`        | `FLUREE_RAFT_NODE_ID`          | yes (stable `u64`)       |
| `--raft-storage-path`   | `FLUREE_RAFT_STORAGE_PATH`     | yes                      |
| `--raft-listen-addr`    | `FLUREE_RAFT_LISTEN_ADDR`      | yes                      |

`--raft-storage-path` must point at a directory on **durable** local storage (not tmpfs). Losing this directory loses committed transactions for *this* node — though as long as a quorum still has its log, openraft can re-deliver the lost state when the node is re-added with the same id.

`--raft-storage-path` must also be disjoint from `--storage-path` (see [Storage layering](#storage-layering)).

The same options are accepted in `.fluree/config.toml`:

```toml
[server.raft]
enabled      = true
node_id      = 1
storage_path = "/var/lib/fluree/raft"
listen_addr  = "0.0.0.0:9090"
```

CLI / env override file values; file values override defaults.

### Storage backend choice

Raft mode is orthogonal to the content-addressed storage choice. In production we recommend shared cloud storage so the **commit blobs the leader writes are immediately visible to every follower's reads** through the same backend. Concretely:

- **AWS** — point all nodes at the same S3 bucket(s) via `--connection-config /etc/fluree/s3.jsonld`. Use a shared DynamoDB nameservice for non-Raft profiles; in Raft mode the **DynamoDB nameservice's writes are unused** (the Raft state machine is the authoritative nameservice), but the storage half stays shared.
- **On-prem** — point all nodes at the same NFS / object-store mount via `--storage-path /shared/fluree`.

The `RaftNameService` on each node sees committed log state; the CAS backend just has to be reachable from every node.

### A complete per-node startup

```bash
fluree-server \
  --listen-addr 0.0.0.0:8080 \
  --connection-config /etc/fluree/s3.jsonld \
  --raft-enabled \
  --raft-node-id 1 \
  --raft-storage-path /var/lib/fluree/raft \
  --raft-listen-addr 0.0.0.0:9090
```

After this the node is **idle** — it has joined no cluster yet. Subsequent admin calls form the cluster.

## Bootstrapping a 3-node cluster

The general shape is:

1. Start `fluree-server` on every node with its per-node config.
2. On **one** node — call it node 1 — initialize the cluster as a single-voter cluster (node 1 auto-elects itself).
3. Add nodes 2 and 3 as **learners**; the leader replicates the existing log to each new peer.
4. Promote the learners to **voters** so they participate in quorum.

The cluster is now a 3-node Raft group. Future membership changes reuse steps 3–4.

The cleanest way to drive these is the `fluree cluster` CLI subcommand, which wraps the private `/cluster/*` HTTP endpoints. The CLI does not need to be installed on the cluster nodes themselves — it just needs network reachability to each node's admin URL. Run it from your bastion / operator workstation over SSH tunnels or via VPC peering.

```bash
# 1. Start every node first (see "A complete per-node startup" above).

# 2. Initialize node 1 as the seed (single-voter cluster).
fluree cluster init \
  --addr       http://node-1-private:9090 \
  --node-id    1 \
  --raft-url   http://node-1-private:9090/raft \
  --client-url http://node-1:8080

# 3. Add nodes 2 and 3 as learners, blocking on catch-up so the
#    follow-up promote can't race replication.
fluree cluster add \
  --leader     http://node-1-private:9090 \
  --node-id    2 \
  --raft-url   http://node-2-private:9090/raft \
  --client-url http://node-2:8080

fluree cluster add \
  --leader     http://node-1-private:9090 \
  --node-id    3 \
  --raft-url   http://node-3-private:9090/raft \
  --client-url http://node-3:8080

# 4. Promote both learners into the voting set.
fluree cluster promote \
  --leader  http://node-1-private:9090 \
  --members 1,2,3

# 5. Confirm.
fluree cluster status --addr http://node-1-private:9090
```

After step 5 you should see `voters: 1, 2, 3`, a non-empty `leader`, and a non-zero `term`.

If you'd rather script the bootstrap directly against the HTTP surface, each CLI command is a single POST or GET — see the [Admin HTTP surface](#admin-http-surface) section below.

## Day-2 operations

### Adding a node

Same recipe as steps 3–4 of the bootstrap: start the new node, `fluree cluster add` against the current leader (blocking), then `promote` with the new voter set.

### Removing a node

`promote --members 1,2 --retain` keeps the dropped voter as a learner (useful for graceful drain); `promote --members 1,2` (no `--retain`) removes it entirely.

### Restarting a node

Restart in place. openraft re-reads its log and snapshots from `--raft-storage-path` and rejoins automatically once it can reach the leader. The node's HTTP listener is up immediately, but its `RaftNameService` returns stale data until the log catches up — clients targeting that node will see slightly older reads during the gap.

### Recovering a failed node

If a node's Raft storage is lost or corrupted, delete the contents of `--raft-storage-path` on that node, restart it, then on the current leader call `fluree cluster add` with the **same** `--node-id`. The leader will deliver the full log (or a snapshot if the log has been truncated past the join point) to bring the node current. `promote` to re-add it to the voter set when it's caught up.

### Configuration changes

Changes to client-facing config (auth, CORS, indexing thresholds) take effect on next restart and don't need cluster coordination — restart nodes one at a time, observing `fluree cluster status` between restarts to make sure quorum is preserved.

`--raft-node-id` and `--raft-listen-addr` **must not change** for a running node. If you really need to change them, treat it as remove + re-add.

### Rolling upgrades

The Raft log and snapshot encodings (postcard for `Command`, `Response`, and `NameServiceState`) are **not versioned** across binary versions. A node running a different binary may fail to deserialize entries from the leader and crash on startup. Roll upgrades one node at a time:

1. Check `fluree cluster status` — confirm the cluster is healthy (correct voter set, recent `last_applied`).
2. Stop one **follower**; upgrade and restart it. Wait for its `last_applied` to catch up to the others.
3. Repeat for the remaining followers.
4. Trigger a leadership transfer (stop and restart the current leader); upgrade and restart it.

Skipping versions is not supported. The safe path is N → N+1; for larger jumps, do them sequentially.

## Admin HTTP surface

All four `fluree cluster` actions map 1:1 to a single HTTP call. Useful when scripting in environments where the CLI isn't available.

When `--admin-auth-mode` is set on a node, these endpoints require an admin token. The CLI does not currently mint admin tokens itself — operators either disable admin auth on the Raft port (acceptable when the listener is behind a trusted network boundary) or front it with a proxy that injects the token.

### `POST /cluster/initialize`

Once, on the seed node, to bootstrap a single-voter cluster:

```bash
curl -X POST http://node-1-private:9090/cluster/initialize \
  -H 'Content-Type: application/json' \
  -d '{
        "members": {
          "1": {
            "raft_addr":   "http://node-1-private:9090/raft",
            "client_addr": "http://node-1:8080"
          }
        }
      }'
```

Returns `204 No Content` on success. `409 Conflict` if the cluster has already been initialized.

### `POST /cluster/add-learner`

Against the leader, for each new peer:

```bash
curl -X POST http://node-1-private:9090/cluster/add-learner \
  -H 'Content-Type: application/json' \
  -d '{
        "node_id":     2,
        "raft_addr":   "http://node-2-private:9090/raft",
        "client_addr": "http://node-2:8080",
        "blocking":    true
      }'
```

Returns `204 No Content` when the learner has caught up (with `blocking: true`). `421 Misdirected Request` if you hit a follower — the response body names the current leader.

### `POST /cluster/change-membership`

Against the leader:

```bash
curl -X POST http://node-1-private:9090/cluster/change-membership \
  -H 'Content-Type: application/json' \
  -d '{ "members": [1, 2, 3], "retain": false }'
```

`retain: true` keeps dropped voters as learners; `false` (default) removes them.

### `GET /cluster/status`

Against any node:

```bash
curl http://node-1-private:9090/cluster/status
```

Returns JSON:

```json
{
  "current_leader": 1,
  "current_term": 4,
  "last_applied_index": 1283,
  "voters": [1, 2, 3],
  "learners": []
}
```

## Client traffic and follower forwarding

Clients can hit **any** node on its client port. Reads always serve locally. Writes (transact, push, branch admin) are inspected by the follower-forward middleware: if the receiving node is the current leader, the request is handled in place; otherwise it's re-issued against the leader's `client_addr` (looked up from the membership the cluster replicates) and the leader's response is relayed back to the client verbatim. From the client's perspective it's a single round-trip — the extra hop lives inside the cluster.

A forwarded request carries an `x-fluree-raft-forward-hops` header. The forwarder bails with `508 Loop Detected` once the count reaches **2** — the slack absorbs an at-most-one stale membership view across a leader transition. Anything beyond is almost certainly a converging cluster, and the client should see a `503` + retry rather than a runaway forward chain.

If no leader is currently elected (mid-election), follower nodes return `503 Service Unavailable` with body `"no leader currently elected; retry shortly"`. Standard client retry/backoff handles this.

## Failure modes

### No leader elected (election in progress)

**Symptom:** Writes to any node return `503` with body `"no leader currently elected; retry shortly"`. Reads succeed against each node's last-applied state.

**When it happens:** Brief leadership transitions (heartbeat miss + election round). Normally bounded to a few hundred milliseconds on a healthy LAN. Persistent `503` means a real partition or majority loss.

**Operator action:** Standard client retry/backoff. Persistent failure → check `fluree cluster status` from every node to identify which side of the partition you're on.

### Quorum lost (minority survives)

**Symptom:** No leader elects. Writes are unavailable. Reads continue to serve from each surviving node's last-applied state but will fall behind whatever the partitioned-off nodes were still applying.

**When it happens:** Loss of `floor(N/2) + 1` voters simultaneously.

**Operator action:** Restore the lost nodes. There is no safe automated way to elect a leader from a minority — doing so risks a split-brain when the partitioned voters return. If the lost nodes are unrecoverable, a manual reset is possible (see [Recovering a failed node](#recovering-a-failed-node)) but should be treated as a recovery operation, not a routine fix.

### Stale leader pointer on a follower

**Symptom:** A follower attempts to forward, can't find a usable `client_addr` for the leader id it sees, and returns `503` with body `"leader node {id} has no usable client address"`.

**When it happens:** Brief window during a membership change where the follower's local view of the cluster lags the new membership.

**Operator action:** Client retry. Persistent failure → confirm via `fluree cluster status` that membership has converged.

### Forwarded request timeout

**Symptom:** Follower returns `504 Gateway Timeout`.

**When it happens:** The follower → leader forwarding budget is a hard-coded **60 seconds** for the full round-trip (connect through response body). Hit when the leader is alive enough to accept the connection but stuck behind a slow stage (e.g. a large transaction blocked on indexing) or a wedged CAS write.

**Operator action:** Client retry with backoff. If the leader is chronically slow, investigate leader-side metrics (queue depth, stage latency) before assuming a Raft-layer problem.

### Hop-limit exceeded

**Symptom:** Follower returns `508 Loop Detected`.

**When it happens:** Two nodes each believe the other is leader (e.g. across a membership-update race), or a misconfigured cluster.

**Operator action:** Confirm via `fluree cluster status` from each node. The hop-limit is **2**, so this requires sustained disagreement.

### Idempotent retry collision

**Symptom:** Two concurrent submissions with the same idempotency key both forward to the leader; the second receives the cached outcome of the first without re-executing.

**When it happens:** By design. The replicated idempotency cache deduplicates on `(ledger_id, idempotency_key)`.

**Operator action:** None — this is correct behavior. Note the cache TTL is **1 hour**; retries after that may execute a second time. Clients needing tighter at-most-once semantics should bound their retry window accordingly.

### Branch lifecycle abort

**Symptom:** Client receives a typed `SubmissionError::Execution { status, ... }` with HTTP status:

- `409 Conflict` — branch head was reset while the request was queued.
- `410 Gone` — branch was dropped, purged, or retracted while queued.
- `422 Unprocessable Entity` — staging failed (policy denial, malformed body, conflict outcome).

**When it happens:** The queue can outlive the branch state it targets. Admin operations and concurrent writes can invalidate queued work.

**Operator action:** None at the cluster layer — these surface to clients as ordinary application-level errors.

### Snapshot rejected

**Symptom:** A follower logs `StorageError::Corruption` while receiving an install-snapshot RPC and refuses to apply it.

**When it happens:** Either a corrupt snapshot stream, or a peer pushing a snapshot with an unsafe id (empty, `..`, > 128 bytes, or characters outside `[A-Za-z0-9_.-]`). The id validation is a path-traversal guard — a peer cannot make this happen accidentally on a clean cluster.

**Operator action:** Treat as a security signal — investigate the source peer before resuming.

### Single-node storage corruption

**Symptom:** A node fails on startup deserializing its snapshot or log.

**When it happens:** Disk corruption, partial writes that bypassed the atomic write (e.g. the underlying filesystem lost an fsync), or a binary downgrade across an incompatible postcard schema change.

**Operator action:** Wipe `--raft-storage-path` on the affected node and re-join as a learner (see [Recovering a failed node](#recovering-a-failed-node)). The cluster continues operating with `N-1` while you do this.

### Body too large on the Raft port

**Symptom:** Peer logs report `413 Payload Too Large` from the Raft listener.

**When it happens:** The per-route body limits on the Raft port are **hard-coded** (vote 1 MiB, append-entries 64 MiB, install-snapshot 1 GiB). A real append-entries batch that exceeds 64 MiB is unusual; reaching it usually means an oversized commit blob was somehow encoded inline (it shouldn't be — commit payloads ride CAS, not the log).

**Operator action:** Investigate the leader's queue for an over-sized envelope. There is no operator knob for these limits in the current build.

### Mixed-version cluster

**Symptom:** A newly-restarted node panics on deserialization of a log entry or snapshot.

**When it happens:** Binary versions across the cluster differ enough that `Command` / `NameServiceState` encodings disagree.

**Operator action:** Roll back the upgraded node; perform the upgrade one-node-at-a-time and only across adjacent versions. See [Rolling upgrades](#rolling-upgrades).

## Operational constraints and limits

### Cluster sizing

Raft requires an odd voter count for liveness. Recommended:

| Voters | Tolerates failed voters | Notes                                    |
| ------ | ----------------------- | ---------------------------------------- |
| 1      | 0                       | No fault tolerance. Prefer non-Raft mode. |
| 3      | 1                       | Smallest production-grade cluster.       |
| 5      | 2                       | Recommended for multi-AZ deployments.    |
| 7      | 3                       | Diminishing returns; commit latency rises with replication fan-out. |

Even-numbered voter counts are inadvisable: the failure budget is the same as the next-smaller odd cluster, but commit latency is worse and an exact split is possible.

Learners do not count toward quorum and can be added freely.

### Stable identities

Both `--raft-node-id` and `--raft-listen-addr` must be stable for a given node. The openraft log and snapshots are keyed by node id; changing either at runtime is treated as a new peer joining and a ghost peer remaining in membership. Bake them into each host's deployment config; if you really must change them, treat the operation as remove + re-add.

### Durable local storage

`--raft-storage-path` must be on durable storage — not tmpfs, not a ramdisk, not ephemeral container storage. The vote, log, and snapshots are all required to survive a restart; losing them on a node means re-joining that node from scratch.

### Disjoint storage paths

`--raft-storage-path` and `--storage-path` (the ledger CAS path, when using the file backend) must be different directories, with neither nested under the other. Startup validation rejects overlaps.

### Membership change ordering

The safe sequence:

1. Bootstrap node 1: `cluster init` (single-voter cluster, auto-elects).
2. Add learner: `cluster add --blocking true` against the leader, waits for catch-up.
3. Promote: `cluster promote --members <new voter set>` against the leader.
4. Repeat 2–3 for each additional node.

`--blocking true` (the default on `cluster add`) is what prevents a promote-before-replicated race that could leave the cluster un-quorumed.

### Idempotency cache TTL

- Cache TTL: **1 hour** (default).
- Eviction tick: **60 s** (leader-only).
- Marker TTL (admin clear): **5 minutes**.

Clients that need stricter at-most-once semantics must keep their retry windows well inside the cache TTL. These TTLs are compile-time defaults today; no operator-facing flags expose them.

### Network transport defaults

| Setting                       | Default        | Source                                              |
| ----------------------------- | -------------- | --------------------------------------------------- |
| RPC timeout (vote, append)    | 500 ms         | `NetworkConfig::default`                            |
| Snapshot install timeout      | 30 s           | `NetworkConfig::default`                            |
| Connect timeout               | 250 ms         | `NetworkConfig::default`                            |
| Vote body limit               | 1 MiB          | per-route hard limit                                 |
| Append-entries body limit     | 64 MiB         | per-route hard limit                                 |
| Install-snapshot body limit   | 1 GiB          | per-route hard limit                                 |
| Follower → leader forward     | 60 s           | hard-coded in the forwarder                          |
| Forward hop limit             | 2              | hard-coded in the forwarder                          |
| Per-attempt waiter timeout    | 8 s            | `QueuedTransactor` default                           |
| Idempotency cache TTL         | 1 h            | `EvictionScheduler` default                          |

These are not currently exposed as server-config flags. Larger clusters that hit the per-route body limits should raise the issue with the maintainers rather than patching local builds — the limits are tuned against a buffered HTTP body model and changing them blindly can OOM nodes during catch-up.

### What survives a leader change

| State                            | Survives? | Notes                                                                                       |
| -------------------------------- | --------- | ------------------------------------------------------------------------------------------- |
| Branch heads, ledger registry    | Yes       | Replicated via the log.                                                                     |
| Queue entries (`EnqueueCommand`) | Yes       | Replicated. The new leader's `CommitWorker` picks up where the old one left off.            |
| Idempotency cache                | Yes       | Replicated.                                                                                 |
| In-flight staged receipts        | **No**    | `StagedReceiptMap` is per-process. Receipts from in-flight stages on the old leader are lost on transition; the apply path falls back to `AppliedReceipt::Minimal` (head identity only). |
| Per-attempt waiters              | **No**    | `WaiterMap` is per-process. Clients waiting on a forwarded request see the forwarding hop time out (`504`) and must retry. Idempotent submissions retry safely; anonymous ones do not. |

## Security notes

- **`/raft/*` inter-node RPC has no built-in authentication.** Peers authenticate to one another via network trust (security groups, VPC ACLs, host firewall rules). Never expose the Raft port to the public internet — a compromised peer that can speak the RPC protocol is already inside the cluster's trust boundary.
- **`/cluster/*` admin endpoints are gated by `--admin-auth-mode` when set.** When admin auth is disabled (the default), the endpoints inherit the same network-trust assumption as the RPC. When admin auth is enabled, requests must carry a valid admin token; the `fluree cluster` CLI does not currently mint admin tokens, so operators using auth must either disable it on the Raft port or front it with a proxy that injects credentials.
- **Snapshot ids from peers are validated** before any path is constructed under the snapshots directory: empty, `..`, > 128 bytes, or characters outside `[A-Za-z0-9_.-]` are rejected. A peer cannot use a malformed snapshot id to escape the snapshots subtree.
- **Body size limits on the Raft port are enforced per-route** (vote 1 MiB, append-entries 64 MiB, install-snapshot 1 GiB) — bounded to prevent a compromised peer from forcing oversized buffer allocation. They are not configurable in the current build.
- **The `--listen-addr` (client) port is governed by the same auth knobs as the single-node transaction server** (`--events-auth-*`, `--data-auth-*`, `--admin-auth-*`). Forwarded requests preserve client auth headers across the follower→leader hop — the leader re-evaluates auth against the same trusted-issuer set, so the auth boundary is unchanged.
- **Outbound HTTP redirects on the Raft transport are disabled** to close SSRF via a 302 to an internal address (such as the EC2 instance-metadata service).
