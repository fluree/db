# Nameservice Schema v2 Design

**Schema Version: 2**

## Overview

This document describes the design for a unified nameservice schema that supports:

1. **Ledgers** with named graphs and independent indexing
2. **Non-ledger graph sources** (indexes/mappings like BM25, Iceberg/R2RML, Vector/HNSW, JDBC, etc.) with varying versioning semantics
3. **Four independent atomic concerns** that can be updated without contention
4. **Watermarked updates** for client subscription and push notifications
5. **Pluggable backends** (DynamoDB, S3, filesystem) with consistent semantics

Terminology:
- Prefer **graph source** in docs and user-facing API descriptions.
- Non-ledger data sources (BM25, vector, Iceberg, R2RML) are called **graph sources**.

## Design Goals

- **Stable schema**: Minimize attribute changes as features evolve
- **Flexible payloads**: Use JSON Maps for evolving/variable content
- **Reduced conflict probability**: Logically independent concerns minimize contention
- **Client subscriptions**: Watermarks enable efficient change detection
- **Coordination via status**: Soft locks/leases for distributed process coordination

---

## The Four Concerns Model

Each nameservice record has four independent concerns, each with its own watermark and payload:

| # | Concern | Watermark | Payload | Updated By |
|---|---------|-----------|---------|------------|
| 1 | **Head** | `commit_t` | `commit` | Transactor (on commit) |
| 2 | **Index** | `index_t` | `index` | Indexer (on index publish) |
| 3 | **Status** | `status_v` | `status` | Various (state changes, metrics, locks) |
| 4 | **Config** | `config_v` | `config` | Admin (settings changes) |

Each concern can be pushed independently without affecting or contending with the others.

---

## DynamoDB Schema

### Table Name

`fluree-nameservice` (configurable)

### Physical layout: item-per-concern (PK+SK)

DynamoDB serializes writes per *item*, not per attribute. To achieve true per-concern independence (transactor vs indexer vs admin), represent each concern as a **separate item** under the same address partition:

- `pk` (partition key): record address in the `name:branch` form (e.g., `"mydb:main"`, `"products-search:main"`)
- `sk` (sort key): concern discriminator

Recommended `sk` values:
- `meta`
- `head` (ledgers only)
- `index` (ledgers + graph sources)
- `config` (ledgers + graph sources)
- `status` (ledgers + graph sources)

This layout aligns with the file-backed v2 pattern (`.index.json` separate) while also eliminating DynamoDB physical contention between writers.

### Design Note: Per-Concern Independence

Each concern is logically independent:
- **No shared `updated_at`**: Each concern’s watermark (`commit_t`, `index_t`, etc.) serves as its timestamp/version marker
- **Disjoint items**: Updating one concern does not touch any attributes of another concern
- **Reduced conflict probability**: Independent concerns minimize logical contention

With the item-per-concern layout, DynamoDB contention is limited to writers of the **same concern**.

### Entity kinds and graph source types

The `meta` item carries the record discriminator:
- `kind`: `ledger` | `graph_source`
- `source_type` (graph sources only): a type string (e.g., `f:Bm25Index`, `f:HnswIndex`, `f:IcebergSource`, `f:R2rmlSource`, `f:JdbcSource`)

Use `graph_source` naming consistently in `pk` values and type strings.

---

## Watermark Semantics

Watermarks are **strict monotonic** per concern. This ensures:
1. Clients can detect changes by comparing watermarks.
2. No change is ever "invisible" to subscribers.
3. Simple comparison logic: `if remote_watermark > local_watermark then changed`.

### commit_t (Ledger commit watermark)

- **Value**: Equals the commit `t` (transaction time).
- **Update rule**: Strict monotonic (`new_t > current_t`).
- **Rationale**: Commits are already strictly ordered by `t`, so `t` IS the version

### index_t (Index watermark)

- **Value**: Transaction time `t` that the published index covers.
- **Update rule**: Strict monotonic (`new_t > current_t`).
- **Admin reindex**: allow idempotent overwrite at the same `t` (`new_t >= current_t`) when rebuilding an index to the same watermark with a new address.

### status_v (Status Watermark)

- **Value**: Atomic incrementing integer
- **Update rule**: Strict monotonic (`new_v > current_v`)
- **Rationale**: Status has no `t` relation; version is just a change counter

### config_v (Config Watermark)

- **Value**: Atomic incrementing integer
- **Update rule**: Strict monotonic (`new_v > current_v`)
- **Rationale**: Config has no `t` relation; version is just a change counter

### Unborn State Semantics

When a record is initialized but has no data yet for a concern:

| Concern | Unborn Watermark | Unborn Payload | Meaning |
|---------|------------------|----------------|---------|
| `head` | `commit_t = 0` | `commit = null` | Ledger initialized, no commits yet |
| `index` | `index_t = 0` | `index = null` | No index published yet |
| `status` | `status_v = 1` | `status = {state: "ready"}` | Always has initial status |
| `config` | `config_v = 0` | `config = null` | No config set yet |

**Key distinction**:
- `*_v = 0` with `payload = null`: Initialized but unborn (record exists)
- Record not found (GetItem returns nothing): Unknown/never created

---

## Payload Schemas

### commit (Ledger)

```json
{
  "id": "bafybeigdyr...commitCid",
  "t": 42
}
```

| Field | Type | Description |
|-------|------|-------------|
| `id` | String | ContentId (CIDv1) of the commit |
| `t` | Number | Transaction time (redundant with `commit_t` but explicit) |

> See [ContentId and ContentStore](content-id-and-contentstore.md) for details on the CID format.

### index (Ledger with Named Graphs)

```json
{
  "default": {
    "id": "bafybeig...indexRootDefault",
    "t": 42,
    "rev": 0
  },
  "txn-metadata": {
    "id": "bafybeig...indexRootTxnMeta",
    "t": 42,
    "rev": 1
  },
  "audit-log": null
}
```

| Field | Type | Description |
|-------|------|-------------|
| `{named-graph}` | Object \| null | Index state per named graph |
| `.id` | String | ContentId (CIDv1) of the index root |
| `.t` | Number | Transaction time the index covers |
| `.rev` | Number | Revision at that `t` (0, 1, 2... for reindex operations) |

**Named graph = `null`** means that graph exists but hasn't been indexed yet.

### index (Graph Source)

For graph sources with index state (e.g., BM25, vector, spatial, Iceberg, etc.), the nameservice stores a **head pointer** to the graph source's latest index root/manifest. The payload is intentionally **opaque** to nameservice: the graph source implementation defines what the ContentId points to and how (or whether) it supports time travel.

```json
{
  "id": "bafybeig...graphSourceIndexRoot",
  "index_t": 42
}
```

For graph sources with no index concept (e.g., JDBC mappings): `null`.

**Design note**: Snapshot history (if any) is stored in **graph-source-owned manifests in storage**, not in nameservice. See `docs/design/graph-source-index-manifests.md`.

### status

```json
{
  "state": "ready",
  "queue_depth": 3,
  "last_commit_ms": 45
}
```

| Field | Type | Description |
|-------|------|-------------|
| `state` | String | Current state (see State Values below) |
| `*` | Any | Additional metadata varies by state and entity type |

#### State Values

| State | Description | Typical Metadata |
|-------|-------------|------------------|
| `ready` | Normal operating state (default initial state) | `queue_depth`, `last_commit_ms` |
| `indexing` | Background indexing in progress | `index_lock` |
| `reindexing` | Full reindex in progress | `reindex_lock`, `progress` |
| `syncing` | Graph source syncing from source | `progress`, `source_t`, `synced_t` |
| `maintenance` | Administrative maintenance in progress | `maintenance_lock` |
| `retracted` | Soft-deleted | `retracted_at`, `reason` |
| `error` | Error state | `error`, `error_at` |

### status with Locks (Coordination)

```json
{
  "state": "indexing",
  "index_lock": {
    "holder": "indexer-7f3a",
    "target_t": 45,
    "acquired_at": 1705312200,
    "expires_at": 1705316100
  }
}
```

| Field | Type | Description |
|-------|------|-------------|
| `index_lock` | Object \| null | Soft lock for indexing coordination |
| `.holder` | String | Identifier of the process holding the lock |
| `.target_t` | Number | The `t` being indexed |
| `.acquired_at` | Number | Unix epoch when lock was acquired |
| `.expires_at` | Number | Unix epoch when lock expires (lease timeout) |

### config

```json
{
  "default_context_id": "bafkreih...contextCid",
  "index_threshold": 1000,
  "replication": {
    "factor": 3,
    "regions": ["us-east-1", "us-west-2"]
  }
}
```

Config is fully flexible JSON. Common fields:

| Field | Type | Description |
|-------|------|-------------|
| `default_context_id` | String | ContentId (CIDv1) of default JSON-LD context |
| `index_threshold` | Number | Commits before auto-index |
| `replication` | Object | Replication settings |

For graph sources, config contains type-specific settings:

**BM25:**
```json
{
  "k1": 1.2,
  "b": 0.75,
  "fields": ["title", "body", "description"]
}
```

**JDBC:**
```json
{
  "connection_string": "jdbc:postgresql://host:5432/db",
  "schema": "public",
  "pool_size": 10
}
```

---

## DynamoDB Operations

### CAS Semantics (Git-like Push)

All push operations support **compare-and-set (CAS)** semantics with expected old values. This enables Git-like divergence detection:

- Caller provides `expected` (the last-known state) and `new` (the desired state)
- Backend rejects if current state doesn't match `expected`
- On rejection, backend returns `actual` current state for caller to reconcile

This is stronger than simple watermark monotonicity: it detects divergence, not just staleness.

### Create (Initialize)

```
Operation: PutItem
ConditionExpression: attribute_not_exists(#pk)
Item: {
  pk: "mydb:main",
  sk: "meta",
  schema: 2,
  kind: "ledger",
  name: "mydb",
  branch: "main",
  dependencies: null,
  created_at: <now>,          // optional
  updated_at_ms: <now_ms>,
  retracted: false,
}
```

### push_commit (Publish Commit)

**Option A: Monotonic only** (simpler, allows fast-forward by any newer commit)
```
Operation: UpdateItem
Key: { pk: "mydb:main", sk: "head" }
ConditionExpression: attribute_not_exists(#ct) OR #ct < :new_t
UpdateExpression: SET #ct = :new_t, #c = :commit
ExpressionAttributeNames: {
  "#ct": "commit_t",
  "#c": "commit"
}
ExpressionAttributeValues: {
  ":new_t": 42,
  ":commit": { "id": "bafybeig...commitT42", "t": 42 }
}
```

**Option B: CAS with expected value** (Git-like, detects divergence)

CAS checks both watermark equality AND payload equality. The condition is a single OR'd expression handling both existing and unborn cases:

```
Operation: UpdateItem
Key: { pk: "mydb:main", sk: "head" }

// Single condition: existing case OR unborn case
ConditionExpression:
  (#ct = :expected_t AND #c = :expected_commit AND :new_t > :expected_t)
  OR
  (#ct = :zero AND attribute_type(#c, :null_type) AND :new_t > :zero)

UpdateExpression: SET #ct = :new_t, #c = :commit
ExpressionAttributeNames: {
  "#ct": "commit_t",
  "#c": "commit"
}
ExpressionAttributeValues: {
  ":expected_t": 41,                                              // caller's last-known watermark
  ":expected_commit": { "id": "bafybeig...commitT41", "t": 41 },  // caller's last-known payload
  ":new_t": 42,
  ":commit": { "id": "bafybeig...commitT42", "t": 42 },
  ":zero": 0,
  ":null_type": "NULL"
}
```

**Caller logic**: Set `:expected_v` and `:expected_head` based on last-known state:
- If unborn: `:expected_v = 0`, `:expected_head` can be any value (the unborn clause matches on `#hv = :zero`)
- If existing: `:expected_v = last_v`, `:expected_head = last_payload`

**Note**: DynamoDB *does* support nested paths like `#h.#addr` (with `#h=head`, `#addr=address`). However, comparing the entire map (`#h = :expected_head`) is simpler and avoids partial-match edge cases.

**Recommendation**: Use Option B (CAS) for transactors to detect divergence. Use Option A for distributed sync where fast-forward is acceptable.

### push_index (Publish Index)

**CAS with expected watermark + monotonic enforcement:**
```
Operation: UpdateItem
Key: { pk: "mydb:main", sk: "index" }
ConditionExpression: (attribute_not_exists(#it) OR #it < :new_t)
UpdateExpression: SET #it = :new_t, #i = :index
ExpressionAttributeNames: {
  "#it": "index_t",
  "#i": "index"
}
ExpressionAttributeValues: {
  ":new_t": 42,
  ":index": {
    "default": { "id": "bafybeig...indexDefault", "t": 42, "rev": 0 },
    "txn-metadata": { "id": "bafybeig...indexTxnMeta", "t": 42, "rev": 1 }
  },
}
```

**Note**: For admin rebuilds at the same watermark, allow `#it <= :new_t` as the condition (idempotent overwrite at equal `t`).

### push_status (Update Status)

```
Operation: UpdateItem
Key: { pk: "mydb:main", sk: "status" }
ConditionExpression: (#sv = :expected_v AND :new_v > :expected_v)
                     OR
                     (attribute_not_exists(#sv) AND :expected_v = :zero)
UpdateExpression: SET #sv = :new_v, #s = :status
ExpressionAttributeNames: {
  "#sv": "status_v",
  "#s": "status"
}
ExpressionAttributeValues: {
  ":expected_v": 89,
  ":zero": 0,
  ":new_v": 90,
  ":status": { "state": "ready", "queue_depth": 0 }
}
```

**Note**: `status_v` starts at 1 (not 0) on creation, so `attribute_not_exists(#sv)` handles cases where the attribute is missing (e.g., partially-written or manually-created items). Normal updates use the first clause.

### push_config (Update Config)

```
Operation: UpdateItem
Key: { pk: "mydb:main", sk: "config" }
ConditionExpression: (#cv = :expected_v AND :new_v > :expected_v)
                     OR
                     (#cv = :zero AND attribute_type(#c, :null_type) AND :expected_v = :zero)
UpdateExpression: SET #cv = :new_v, #c = :config
ExpressionAttributeNames: {
  "#cv": "config_v",
  "#c": "config"
}
ExpressionAttributeValues: {
  ":expected_v": 2,
  ":zero": 0,
  ":new_v": 3,
  ":config": { "default_context_id": "bafkreih...", "index_threshold": 500 },
  ":null_type": "NULL"
}
```

**Note**: Unborn clause checks both `#cv = :zero` AND `attribute_type(#c, NULL)` to prevent accepting writes against inconsistent states.

### Retract

```
Operation: UpdateItem
Key: { pk: "mydb:main", sk: "meta" }
UpdateExpression: SET #r = :true, #sv = :new_sv, #s = :status
ExpressionAttributeNames: {
  "#r": "retracted",
  "#sv": "status_v",
  "#s": "status"
}
ExpressionAttributeValues: {
  ":true": true,
  ":new_sv": 91,
  ":status": { "state": "retracted", "retracted_at": 1705315800 }
}
```

### Lookup (Read)

```
Operation: GetItem
Key: { pk: "mydb:main", sk: "meta" }
ConsistentRead: true
```

To read full state, query all items for the record address: `pk = "mydb:main"` and assemble `meta + head + index + status + config` as present.

### List by Kind

```
Operation: Query (requires GSI on kind)
KeyConditionExpression: #kind = :kind
ExpressionAttributeNames: { "#kind": "kind" }
ExpressionAttributeValues: { ":kind": "ledger" }
```

To list graph sources, query `kind = graph_source`.

To list graph sources of a specific type (optional GSI), query `source_type = f:Bm25Index`, etc.

---

## Push Result Handling

Each push operation returns one of:

| Result | Meaning | Action |
|--------|---------|--------|
| `Updated` | Update accepted | Proceed |
| `Conflict` | Expected didn't match current | Reconcile using `actual` |

### Rust Types (aligned with existing RefKind/CasResult vocabulary)

```rust
/// Which concern is being read or updated.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum ConcernKind {
    /// The commit head pointer (`commit_t` + `commit` payload)
    Head,
    /// The index state (`index_t` + `index` payload)
    Index,
    /// The status state (status_v + status payload)
    Status,
    /// The config state (config_v + config payload)
    Config,
}

/// Value of a concern: watermark + optional payload.
///
/// - `Some(ConcernValue { v: 0, payload: None })` — unborn (initialized, no data)
/// - `Some(ConcernValue { v: N, payload: Some(...) })` — has data
/// - `None` (at Option level) — record doesn't exist
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ConcernValue<T> {
    pub v: i64,
    pub payload: Option<T>,
}

/// Outcome of a compare-and-set push operation.
///
/// Conflicts are NOT errors — they are expected outcomes of concurrent
/// writes and must be handled by the caller (retry, report, etc.).
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum CasResult<T> {
    /// CAS succeeded — the concern was updated to the new value.
    Updated,
    /// CAS failed — `expected` did not match the current value.
    /// `actual` carries the current concern value so the caller can decide
    /// what to do next (retry, diverge, etc.).
    Conflict { actual: Option<ConcernValue<T>> },
}
```

### Conflict Handling

On `Conflict`, the caller receives the actual current state and can:

1. **Fast-forward**: If `actual.v < new.v`, retry with `expected = actual`
2. **Divergence**: If `actual.v >= new.v` or addresses differ unexpectedly, handle merge/error
3. **Retry loop**: For distributed systems, implement bounded retry with backoff

```rust
async fn push_with_retry<T>(
    ns: &impl ConcernPublisher<T>,
    address: &str,
    kind: ConcernKind,
    new: ConcernValue<T>,
    max_retries: usize,
) -> Result<CasResult<T>> {
    let mut expected = ns.get_concern(address, kind).await?;

    for _ in 0..max_retries {
        match ns.push_concern(address, kind, expected.as_ref(), &new).await? {
            CasResult::Updated => return Ok(CasResult::Updated),
            CasResult::Conflict { actual } => {
                // Check if fast-forward is still possible
                if let Some(ref act) = actual {
                    if new.v <= act.v {
                        // Diverged - can't fast-forward
                        return Ok(CasResult::Conflict { actual });
                    }
                }
                // Retry with new expected
                expected = actual;
            }
        }
    }

    // Exhausted retries
    let actual = ns.get_concern(address, kind).await?;
    Ok(CasResult::Conflict { actual })
}
```

---

## Example Records

### DynamoDB (item-per-concern) examples

This section shows the DynamoDB **physical layout** (multiple items per address partition).
Other backends serialize the same logical concerns differently.

#### Ledger (typical items)

Ledger records are represented as multiple items under the same `pk`:

```json
{
  "pk": "mydb:main",
  "sk": "meta",
  "schema": 2,
  "kind": "ledger",
  "name": "mydb",
  "branch": "main",
  "created_at": 1705312200,
  "updated_at_ms": 1705312200123,
  "retracted": false
}
```

```json
{
  "pk": "mydb:main",
  "sk": "head",
  "schema": 2,
  "commit_t": 42,
  "commit": { "id": "bafybeig...commitT42", "t": 42 }
}
```

```json
{
  "pk": "mydb:main",
  "sk": "index",
  "schema": 2,
  "index_t": 42,
  "index": {
    "default": { "id": "bafybeig...indexDefaultT42", "t": 42, "rev": 0 }
  }
}
```

```json
{
  "pk": "mydb:main",
  "sk": "config",
  "schema": 2,
  "config_v": 2,
  "config": { "default_context_id": "bafkreih...contextCid", "index_threshold": 1000 }
}
```

```json
{
  "pk": "mydb:main",
  "sk": "status",
  "schema": 2,
  "status_v": 89,
  "status": { "state": "ready", "queue_depth": 3, "last_commit_ms": 45 }
}
```

#### Ledger (unborn)

An "unborn" ledger has all 5 concern items created atomically at initialization. The `head` and `index` items have watermarks set to `0` with null payloads. The `status` item starts at `status_v=1` with `state="ready"`. The `config` item starts at `config_v=0` (unborn).

### Graph Source (BM25)

```json
{
  "pk": "search:main",
  "sk": "meta",
  "schema": 2,
  "kind": "graph_source",
  "source_type": "f:Bm25Index",
  "name": "search",
  "branch": "main",
  "dependencies": ["mydb:main"],
  "created_at": 1705312200,
  "updated_at_ms": 1705312200123,
  "retracted": false
}
```

Additional concern items for the same `pk` (examples):

```json
{
  "pk": "search:main",
  "sk": "config",
  "schema": 2,
  "config_v": 1,
  "config": { "k1": 1.2, "b": 0.75, "fields": ["title", "body"] }
}
```

```json
{
  "pk": "search:main",
  "sk": "index",
  "schema": 2,
  "index_t": 42,
  "index": { "id": "bafybeig...bm25IndexRoot" }
}
```

### Graph Source (Iceberg)

```json
{
  "pk": "analytics:main",
  "sk": "meta",
  "schema": 2,
  "kind": "graph_source",
  "source_type": "f:IcebergSource",
  "name": "analytics",
  "branch": "main",
  "dependencies": ["mydb:main"],
  "created_at": 1705312200,
  "updated_at_ms": 1705312200123,
  "retracted": false,
  "...": "see config/index items"
}
```

### Graph Source (JDBC - No Index)

```json
{
  "pk": "erp:main",
  "sk": "meta",
  "schema": 2,
  "kind": "graph_source",
  "source_type": "f:JdbcSource",
  "name": "erp",
  "branch": "main",
  "dependencies": null,
  "created_at": 1705312200,
  "updated_at_ms": 1705312200123,
  "retracted": false,
  "...": "see config item; index item may be absent or have index_t=0"
}
```

---

## Git-like Push Model

The nameservice follows a git-like model where:

1. **Local nameservice**: Each node has a local NS for reads and local writes
2. **Upstream nameservice**: The "source of truth" that accepts or rejects pushes
3. **Push operations**: Local changes are pushed upstream
4. **Forward operations**: Requests can be forwarded upstream without local write

```
┌─────────────────┐         push_head         ┌─────────────────────┐
│  Transactor     │ ────────────────────────▶ │                     │
│  (local NS)     │                           │   Upstream NS       │
└─────────────────┘                           │                     │
                                              │  - DynamoDB, or     │
┌─────────────────┐         push_index        │  - S3 + ETags, or   │
│  Indexer        │ ────────────────────────▶ │  - FS + locks, or   │
│  (local NS)     │                           │  - Service          │
└─────────────────┘                           │                     │
        ▲                                     │  Enforces:          │
        │              pull/sync              │  - Watermark rules  │
        └─────────────────────────────────────│  - Serialization    │
                                              └─────────────────────┘
```

### Upstream NS Backend Options

| Backend | How It Enforces Rules |
|---------|----------------------|
| **DynamoDB** | Conditional expressions on watermarks |
| **S3** | ETags for CAS + application logic |
| **Filesystem** | File locks or single-writer process |
| **Service** | Queue + application logic |

The push interface is the same regardless of backend.

---

## Status-based Coordination (Soft Locks)

Status can carry soft locks for coordinating distributed processes:

### Lock Acquisition Flow

```
1. Indexer starts up
2. Read current status
3. If index_lock exists and not expired:
     → Another indexer is working, wait or skip
4. If no lock or lock expired:
     → Push status with our lock claim (status_v + 1)
     → If accepted: we own the lock, proceed
     → If rejected: someone else claimed it, back off
5. Do indexing work (periodically refresh lock by pushing status)
6. Push index update
7. Push status: clear lock, set state to ready
```

### Lock Expiry (Crash Recovery)

If a process crashes while holding a lock:
- The `expires_at` timestamp allows other processes to take over
- No manual intervention needed
- Typical lease duration: 5-15 minutes depending on operation

### Lock Refresh

Long-running operations should periodically refresh their lock:

```json
{
  "state": "indexing",
  "index_lock": {
    "holder": "indexer-7f3a",
    "target_t": 45,
    "acquired_at": 1705312200,
    "expires_at": 1705316100,
    "refreshed_at": 1705314000
  },
  "progress": 0.67
}
```

---

## Client Subscription Model

Clients track watermarks to detect changes:

```json
{
  "subscriptions": {
    "mydb:main": {
      "kind": "ledger",
      "commit_t": 42,
      "index_t": 42,
      "status_v": 89,
      "config_v": 2
    },
    "search:main": {
      "kind": "graph_source",
      "source_type": "f:Bm25Index",
      "index_t": 42,
      "status_v": 12,
      "config_v": 1
    }
  }
}
```

### Change Detection

1. Client polls or receives notification
2. Compare watermarks: `if remote.commit_t > local.commit_t`
3. Fetch only the changed concern(s)
4. Update local cache

### Subscription Granularity

Clients can subscribe to:
- **All concerns** for an address
- **Specific concerns** (e.g., only `commit_t` for a query client)
- **All addresses** of a kind (e.g., all ledgers)

---

## File-backed Nameservice Considerations

The logical concerns (head/index/status/config) can be stored in different **physical layouts** depending on the backend.

The file-backed and storage-backed implementations in this repo use the **`ns@v2` JSON-LD** format (see `fluree-db-nameservice/src/file.rs` and `fluree-db-nameservice/src/storage_ns.rs`):
- Main record: `ns@v2/{name}/{branch}.json` (commit/head + status + config-ish fields)
- Index record: `ns@v2/{name}/{branch}.index.json` (index head pointer only)

Field names differ from the DynamoDB layout, but the **semantics match**:
- logical `commit_t` is stored as `f:t`
- logical `commit.id` is stored as `f:ledgerCommit.@id` (a CID string)
- logical `index_t` is stored as `f:ledgerIndex.f:t` (or `f:indexT` for graph source index files)
- logical `index.id` is stored as `f:ledgerIndex.@id` (a CID string, or `f:indexId` for graph source index files)

### Layout Options

**Option A: Single File (Unified)**
```
ns@v2/{name}/{branch}.json
```
- Contains all four concerns in one file
- Simplest for reads (one fetch)
- Requires single-writer discipline or file-level CAS

**Option B: Separate Head and Index Files (Current Implementation)**
```
ns@v2/{name}/{branch}.json        # head + status + config
ns@v2/{name}/{branch}.index.json  # index only
```
- Matches current implementation
- Allows transactor and indexer to write independently
- 2 files to read per entity for full state
- **Trade-off**: Status and config updates contend with head updates at file-lock level. Acceptable if status updates are low-frequency (state changes only, not high-frequency metrics).

**Option C: Fully Separate Files (Maximum Independence)**
```
ns@v2/{name}/{branch}.head.json
ns@v2/{name}/{branch}.index.json
ns@v2/{name}/{branch}.status.json
ns@v2/{name}/{branch}.config.json
```
- Each concern in its own file
- Maximum write independence
- 4 files to read per entity

### Recommended Approach

Use **Option B** (separate head/index) as the default:
- Proven in current implementation
- Solves the main contention issue (transactor vs indexer)
- Reasonable read overhead (2 files)
- **Constraint**: Status updates should be coarse-grained (state transitions, not per-transaction metrics). If high-frequency status updates are needed, consider Option C.

Use **Option C** (fully separate files) when:
- Status updates are frequent (e.g., real-time queue depth reporting)
- Multiple independent processes update different concerns
- Write independence is more important than read efficiency

For **queryable nameservice** with many entities:
- Read files in parallel
- Consider in-memory caching with file-change notification
- The 2-file layout is acceptable; 4-file layout may add too much I/O

### Atomicity Mechanisms

| Backend | Mechanism | Notes |
|---------|-----------|-------|
| Filesystem | Atomic rename (write to temp, rename) | POSIX guarantees |
| S3 | ETags for CAS | `If-Match` header |
| GCS | Generation numbers | Similar to ETags |

### File Content Format

Each file contains JSON matching the concern's payload plus metadata:

**head file** (`{name}/{branch}.json`):
```json
{
  "@context": { "f": "https://ns.flur.ee/db#" },
  "@id": "mydb:main",
  "@type": ["f:Database", "f:LedgerSource"],
  "f:ledger": { "@id": "mydb" },
  "f:branch": "main",
  "f:ledgerCommit": { "@id": "bafybeig...commitT42" },
  "f:t": 42,
  "f:ledgerIndex": { "@id": "bafybeig...indexRootT42", "f:t": 42 },
  "f:status": "ready"
}
```

**index file** (`{name}/{branch}.index.json`):
```json
{
  "@context": { "f": "https://ns.flur.ee/db#" },
  "f:ledgerIndex": { "@id": "bafybeig...indexRootT42", "f:t": 42 }
}
```

---

## Global Secondary Indexes (GSIs)

### GSI1: `gsi1-kind` (Implemented)

| GSI Name | Partition Key | Sort Key | Use Case |
|----------|---------------|----------|----------|
| `gsi1-kind` | `kind` | `pk` | List all entities of a kind (`ledger`, `graph_source`) |

- Only `meta` items carry the `kind` attribute and project into the GSI
- Projection: `INCLUDE` with `name`, `branch`, `source_type`, `dependencies`, `retracted`
- Used by `all_records()` (kind=`ledger`) and `all_vg_records()` (kind=`graph_source`)
- After GSI query returns meta items, `BatchGetItem` fetches remaining concern items (`config`, `index`) to assemble full records

### Future GSIs

| GSI Name | Partition Key | Sort Key | Use Case |
|----------|---------------|----------|----------|
| `source-type-index` | `source_type` | `pk` | List all graph sources of a given type |
| `state-index` | `status_state` | `pk` | Find entities in specific state |

**Note on `state-index`**: DynamoDB GSIs cannot use nested map attributes as keys. To enable this GSI:

1. Add an **optional denormalized attribute** `status_state` (String) on the `status` item
2. Update `status_state` whenever `status.state` changes
3. Only add it if you need GSI-based queries by state

**Alternative**: Use Scan with FilterExpression on `status.state` (less efficient but no schema extension needed)

## Future Considerations

### Streams and Events

DynamoDB Streams can be enabled to:
- Trigger Lambda on changes
- Build event sourcing
- Replicate to other regions

### Multi-region

For global deployments:
- Use DynamoDB Global Tables
- Or regional nameservices with cross-region sync

---

## Appendix: Attribute Reference

All items share:

| Attribute | Type | Description |
|-----------|------|-------------|
| `pk` | String | Record address (`name:branch`) |
| `sk` | String | Concern discriminator (`meta`, `head`, `index`, `status`, `config`) |
| `schema` | Number | Schema version (always `2`) |

### `meta` item

| Attribute | Type | Description |
|-----------|------|-------------|
| `kind` | String | `ledger` \| `graph_source` |
| `name` | String | Base name |
| `branch` | String | Branch name |
| `retracted` | Boolean | Soft-delete flag |
| `branches` | Number | Child branch reference count (0 for leaf branches, omitted when 0 in JSON-LD) |
| `dependencies` | List\<String\> \| null | Graph-source dependencies (optional) |
| `source_type` | String \| null | Graph-source type (e.g., `f:Bm25Index`) |
| `created_at` | Number | Creation timestamp (epoch seconds, optional) |
| `updated_at_ms` | Number | Last update time (epoch millis, optional) |

### `meta` item: Branch Attributes

For branches created via `create_branch`, the `meta` item carries an additional attribute recording the source branch:

| Attribute | Type | Description |
|-----------|------|-------------|
| `bp_source` | String \| null | Source branch name (e.g., `"main"`) |

This attribute is `null`/absent for the original `main` branch. The JSON-LD format uses `f:sourceBranch`. The divergence point between a branch and its source is computed on demand by walking the commit chains rather than being stored.

### `head` item (ledgers only)

| Attribute | Type | Description |
|-----------|------|-------------|
| `commit_t` | Number | Commit watermark (`t`) |
| `commit` | Map \| null | `{ id, t }` (id is a ContentId CID string) |

### `index` item (ledgers + graph sources)

| Attribute | Type | Description |
|-----------|------|-------------|
| `index_t` | Number | Index watermark (`t`) |
| `index` | Map \| null | Ledger index map or graph-source head pointer payload |

### `status` item (ledgers + graph sources)

| Attribute | Type | Description |
|-----------|------|-------------|
| `status_v` | Number | Status change counter |
| `status` | Map | Status payload |
| `status_state` | String \| null | Optional denormalized `status.state` for a GSI |

### `config` item (ledgers + graph sources)

| Attribute | Type | Description |
|-----------|------|-------------|
| `config_v` | Number | Config change counter |
| `config` | Map \| null | Config payload |

### Watermark Semantics Summary

| Watermark | Semantics | Initial Value | Update Rule |
|-----------|-----------|---------------|-------------|
| `commit_t` | = commit `t` | 0 (unborn) | Strict: `new > current` |
| `index_t` | = index `t` | 0 (unborn) | Strict: `new > current` (admin may allow equal) |
| `status_v` | Counter | 1 (ready) | Strict: `new > current` |
| `config_v` | Counter | 0 (unborn) | Strict: `new > current` |
