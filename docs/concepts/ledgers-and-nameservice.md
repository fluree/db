# Ledgers and the Nameservice

Ledgers are Fluree's fundamental unit of data organization—similar to databases in traditional RDBMS systems. The nameservice is the metadata registry that enables ledger discovery, coordination, and management across distributed deployments.

## Ledgers

A **ledger** in Fluree is an independent, versioned graph database containing:

- A complete graph of RDF triples
- Complete transaction history with temporal versioning
- Independent indexing and storage
- Configurable permissions and policies
- Support for multiple branches

### Ledger IDs

Ledgers are identified by **ledger IDs** with the format `ledger-name:branch`.

A ledger ID serves as both a human-readable identifier and the canonical lookup key used across APIs, CLI, and caching.

**Examples:**

- `mydb:main` - Primary branch of the "mydb" ledger
- `customers:dev` - Development branch of the "customers" ledger
- `inventory:prod` - Production branch of the "inventory" ledger
- `tenant/app:feature-x` - Feature branch with hierarchical naming

**Branch Semantics:**

- The `:branch` suffix allows multiple isolated versions of the same logical ledger to coexist
- The default branch name is `main` when not specified (e.g., `mydb` is equivalent to `mydb:main`)
- Branches are independent—changes in one branch don't affect others
- Branch names can include slashes for hierarchical organization

### Ledger Lifecycle

Ledgers are created implicitly through the first transaction and persist until explicitly retracted. Each ledger maintains:

- **Transaction History**: Every change is recorded as a transaction with a unique timestamp (`t`)
- **Current State**: The latest indexed state of all data
- **Novelty Layer**: Uncommitted transactions since the last index
- **Metadata**: Creation time, latest commit, indexing status

**Creation Flow:**

1. First transaction to a ledger ID creates the ledger automatically
2. Transaction is committed and assigned a transaction time (`t`)
3. Commit ID is published to the nameservice
4. Background indexing process creates queryable indexes
5. Index ID is published to the nameservice when complete

**Retraction:**

Ledgers can be marked as retracted (soft delete), which:
- Marks the ledger as inactive in the nameservice
- Preserves storage artifacts
- Prevents normal load/create/write paths from treating the alias as active
- Keeps the alias reserved until an administrator purges or otherwise repairs the nameservice record

## The Nameservice

The **nameservice** is Fluree's metadata registry that enables ledger discovery and coordination. It acts as a directory service, tracking where ledger data is stored and what state each ledger is in.

### Purpose and Role

The nameservice provides:

- **Discovery**: Find ledgers by ledger ID across distributed deployments
- **Coordination**: Track commit and index state for consistency
- **Metadata Management**: Store ledger configuration and status
- **Multi-Process Support**: Enable coordination across multiple Fluree instances

### What the Nameservice Stores

For each ledger, the nameservice maintains a **nameservice record** (`NsRecord`) containing:

#### Core Identifiers

- **`id`**: Canonical ledger ID with branch (e.g., `"mydb:main"`)
- **`name`**: Ledger name without branch suffix (e.g., `"mydb"`)
- **`branch`**: Branch name (e.g., `"main"`)

#### Commit State

- **`commit_id`**: ContentId (CIDv1) of the latest commit
- **`commit_t`**: Transaction time of the latest commit

The commit represents the most recent transaction that has been persisted. Commits are published immediately after each successful transaction. The `commit_id` is a content-addressed identifier derived from the commit's bytes — it is storage-agnostic and does not depend on where the commit is physically stored.

#### Index State

- **`index_id`**: ContentId (CIDv1) of the latest index root
- **`index_t`**: Transaction time of the latest index

The index represents a queryable snapshot of the ledger state. Indexes are created by background processes and may lag behind commits. Like commits, the `index_id` is a content-addressed identifier.

#### Branch Metadata

- **`source_branch`**: For branches created via `create_branch`, records the name of the source branch (e.g., `"main"`). `None` for the initial branch.

The divergence point (common ancestor) between a branch and its source is computed on demand by walking the commit chains rather than being stored. This avoids stale metadata and supports merge scenarios where the relationship between branches changes over time.

#### Additional Metadata

- **`default_context_id`**: ContentId of the default JSON-LD @context for the ledger
- **`retracted`**: Whether the ledger has been marked as inactive

### Commit vs Index: Understanding the Difference

This distinction is crucial for understanding Fluree's architecture:

**Commits (`commit_t`):**
- Created immediately after each transaction
- Represent the transaction log (what changed)
- Small, append-only files
- Published synchronously
- Always up-to-date with latest transactions

**Indexes (`index_t`):**
- Created by background indexing processes
- Represent queryable database snapshots (complete state)
- Large, optimized data structures
- Published asynchronously
- May lag behind commits (this gap is the "novelty layer")

**Example Timeline:**

```text
t=1:  Transaction committed → commit_t=1, index_t=0
t=2:  Transaction committed → commit_t=2, index_t=0
t=3:  Transaction committed → commit_t=3, index_t=0
       [Background indexing completes] → index_t=3
t=4:  Transaction committed → commit_t=4, index_t=3
t=5:  Transaction committed → commit_t=5, index_t=3
       [Novelty layer: t=4, t=5 not yet indexed]
```

Queries combine the indexed state (up to `index_t`) with the novelty layer (transactions between `index_t` and `commit_t`) to provide real-time results.

### Nameservice Operations

The nameservice supports these key operations:

#### Lookup

Find ledger metadata by ledger ID:

```rust
// Pseudo-code
let record = nameservice.lookup("mydb:main").await?;
// Returns: NsRecord with commit_id, index_id, timestamps, etc.
```

#### Publishing

Record new commits and indexes:

- **`RefPublisher::compare_and_set_ref()` / `fast_forward_commit()`**: Advance the commit head with explicit CAS conflict handling
- **`publish_index(ledger_id, index_id, index_t)`**: Update index state (monotonic: only if `new_t > existing_t`)

Commit-head publishing is **CAS-based** so concurrent writers get an explicit conflict result instead of a silent no-op. Index publishing remains monotonic and only accepts updates that advance time forward.

#### Branching

Create and list branches:

- **`create_branch(ledger_name, new_branch, source_branch, at_commit)`**: Create a new branch from the source. When `at_commit` is `None`, the branch starts at the source's current HEAD; when `Some((commit_id, commit_t))`, the branch starts at the supplied historical commit instead (callers are expected to verify reachability from source HEAD before passing it in).
- **`list_branches(ledger_name)`**: List all non-retracted branches for a ledger

#### Discovery

List all available ledgers:

```rust
// Pseudo-code
let all_ledgers = nameservice.all_records().await?;
// Returns: Vec<NsRecord> for all known ledgers
```

### Querying the Nameservice

The nameservice can be queried using standard JSON-LD query or SPARQL syntax. This enables powerful ledger discovery, filtering, and metadata analysis across all managed databases.

#### Rust API (Builder Pattern)

```rust
// Find all ledgers on main branch
let query = json!({
    "@context": {"f": "https://ns.flur.ee/db#"},
    "select": ["?ledger"],
    "where": [{"@id": "?ns", "f:ledger": "?ledger", "f:branch": "main"}]
});

let results = fluree.nameservice_query()
    .jsonld(&query)
    .execute_formatted()
    .await?;

// Query with SPARQL
let results = fluree.nameservice_query()
    .sparql("PREFIX f: <https://ns.flur.ee/db#>
             SELECT ?ledger ?t WHERE {
               ?ns a f:LedgerSource ;
                   f:ledger ?ledger ;
                   f:t ?t
             }")
    .execute_formatted()
    .await?;

// Convenience method (equivalent to builder with defaults)
let results = fluree.query_nameservice(&query).await?;
```

#### HTTP API

```bash
# List ledgers and graph sources from the nameservice
curl http://localhost:8090/v1/fluree/ledgers
```

#### Available Properties

**Ledger Records** (`@type: "f:LedgerSource"`):

| Property | Description |
|----------|-------------|
| `f:ledger` | Ledger name (without branch suffix) |
| `f:branch` | Branch name (e.g., "main", "dev") |
| `f:t` | Current transaction number |
| `f:status` | Status: "ready" or "retracted" |
| `f:ledgerCommit` | Reference to latest commit ContentId |
| `f:ledgerIndex` | Index info object with `@id` (ContentId) and `f:t` |
| `f:sourceBranch` | Source branch name (e.g., `"main"`) if this is a branched ledger |
| `f:defaultContextCid` | Default JSON-LD context ContentId (if set) |

**Graph Source Records** (`@type: "f:GraphSourceDatabase"`):

| Property | Description |
|----------|-------------|
| `f:name` | Graph source name |
| `f:branch` | Branch name |
| `f:status` | Status: "ready" or "retracted" |
| `f:config` | Configuration JSON |
| `f:dependencies` | Array of source ledger dependencies |
| `f:indexId` | Index ContentId |
| `f:indexT` | Index transaction number |

#### Example Queries

**Find all ledgers with t > 100:**
```json
{
  "@context": {"f": "https://ns.flur.ee/db#"},
  "select": ["?ledger", "?t"],
  "where": [
    {"@id": "?ns", "f:ledger": "?ledger", "f:t": "?t"}
  ],
  "filter": ["(> ?t 100)"]
}
```

**Find ledgers by name pattern (hierarchical):**
```json
{
  "@context": {"f": "https://ns.flur.ee/db#"},
  "select": ["?ledger", "?branch"],
  "where": [
    {"@id": "?ns", "f:ledger": "?ledger", "f:branch": "?branch"}
  ],
  "filter": ["(strStarts ?ledger \"tenant1/\")"]
}
```

**Find all BM25 graph sources:**
```json
{
  "@context": {
    "f": "https://ns.flur.ee/db#"
  },
  "select": ["?name", "?deps"],
  "where": [
    {"@id": "?gs", "@type": "f:Bm25Index", "f:name": "?name", "f:dependencies": "?deps"}
  ]
}
```

#### Retraction

Mark ledgers as inactive without deleting storage artifacts:

```rust
// Pseudo-code
nameservice.retract("mydb:old-branch").await?;
// Sets retracted=true; the alias remains reserved.
```

### Storage Backends

The nameservice can be backed by various storage systems, each suited for different deployment scenarios:

#### File System (`FileNameService`)

- **Use Case**: Single-server deployments, development, testing
- **Storage**: Files in `ns@v2/` directory structure
- **Format**: JSON files per ledger (`{ledger}/{branch}.json`)
- **Characteristics**: Simple, local, no external dependencies

#### AWS S3 (`StorageNameService`)

- **Use Case**: Distributed deployments using S3 for both data and metadata
- **Storage**: S3 objects with ETag-based compare-and-swap (CAS)
- **Characteristics**: Scalable, distributed, requires AWS credentials

#### AWS DynamoDB (`DynamoDbNameService`)

- **Use Case**: Distributed deployments needing low-latency metadata coordination
- **Storage**: DynamoDB table with composite-key layout (one item per concern)
- **Format**: Separate items for `meta`, `head`, `index`, `config`, `status` per ledger/graph source
- **Characteristics**: Single-digit millisecond latency, per-concern write independence, conditional expressions for monotonic updates
- See [DynamoDB Nameservice Guide](../operations/dynamodb-guide.md) for setup and schema details

#### Memory (`MemoryNameService`)

- **Use Case**: Testing, in-process applications
- **Storage**: In-memory data structures
- **Format**: No persistence
- **Characteristics**: Fast, ephemeral, process-local

### Graph Sources

The nameservice also tracks **graph sources**—specialized indexes and integrations:

- **BM25**: Full-text search indexes
- **Vector**: Vector similarity search
- **R2RML**: Relational database mappings
- **Iceberg**: Apache Iceberg table integrations

Graph sources have their own nameservice records (`GraphSourceRecord`) with similar metadata but different semantics. See the [Graph Sources](graph-sources.md) documentation for details.

## Example Usage

### Creating a Ledger

Ledgers are created automatically on the first transaction. Specify the ledger ID in your transaction:

```json
POST /insert?ledger=mydb:main
Content-Type: application/json

{
  "@context": {
    "ex": "http://example.org/ns/",
    "foaf": "http://xmlns.com/foaf/0.1/"
  },
  "@graph": [
    {
      "@id": "ex:alice",
      "@type": "foaf:Person",
      "foaf:name": "Alice"
    }
  ]
}
```

**What Happens:**

1. Transaction is processed and committed (assigned `t=1`)
2. Commit is stored and its ContentId published to nameservice
3. Nameservice record created/updated with `commit_t=1`
4. Background indexing begins
5. When indexing completes, `index_t=1` is published

### Querying a Ledger

Specify the ledger ID in your query:

**SPARQL:**

```sparql
PREFIX ex: <http://example.org/ns/>
PREFIX foaf: <http://xmlns.com/foaf/0.1/>

SELECT ?name
FROM <mydb:main>
WHERE {
  ex:alice foaf:name ?name
}
```

The `FROM <mydb:main>` clause specifies which ledger to query. The query engine:
1. Looks up `mydb:main` in the nameservice
2. Retrieves the index ContentId for efficient querying
3. Combines indexed data with novelty layer for current results

**JSON-LD Query:**

```json
{
  "@context": {
    "ex": "http://example.org/ns/",
    "foaf": "http://xmlns.com/foaf/0.1/"
  },
  "select": ["?name"],
  "from": "mydb:main",
  "where": [
    { "@id": "ex:alice", "foaf:name": "?name" }
  ]
}
```

### Checking Ledger Status

Query the nameservice to check ledger state:

```rust
// Pseudo-code
let record = nameservice.lookup("mydb:main").await?;

if let Some(record) = record {
    println!("Latest commit: t={}", record.commit_t);
    println!("Latest index: t={}", record.index_t);
    
    if record.has_novelty() {
        println!("Novelty layer: {} transactions pending index", 
                 record.commit_t - record.index_t);
    }
    
    if record.retracted {
        println!("Ledger is retracted (inactive)");
    }
}
```

### Branching

Branches let you create isolated copies of a ledger's state for independent development. After branching, transactions on one branch are invisible to the other.

#### Creating a Branch

Branches are created from a source branch (default: `main`). The new branch starts at the same transaction time as the source:

```text
mydb:main (t=5)
  └── create_branch("mydb", "dev")
mydb:dev  (t=5)  # starts with same data as main at t=5
```

Branches can also be nested — you can branch from a branch:

```text
mydb:main (t=5)
  └── mydb:dev (t=7)      # branched from main at t=5, then advanced
        └── mydb:feature (t=8)  # branched from dev at t=7, then advanced
```

#### Data Isolation

After branching, each branch has its own independent transaction history:

```text
mydb:main   → t=5 (shared) → t=6: insert Bob   → t=7: insert Dave
mydb:dev    → t=5 (shared) → t=6: insert Carol
```

Querying `main` returns Alice + Bob + Dave. Querying `dev` returns Alice + Carol. Bob and Dave never appear on `dev`; Carol never appears on `main`.

#### Storage Model

Branches share storage efficiently through a **`BranchedContentStore`** — a recursive content store that reads from the branch's own namespace first, then falls back to parent namespaces for pre-branch-point content.

- **Commits are not copied** — historical commits are read from the source namespace via fallback
- **Index files are copied** — protects the branch from garbage collection on the source after reindexing
- **String dictionaries are globally shared** — stored in a per-ledger `@shared` namespace (e.g., `mydb/@shared/dicts/`) rather than per-branch paths, so all branches read and write to the same location without copying or fallback. The `@` prefix cannot collide with branch names. See [Storage Traits — Global Dictionary Storage](../design/storage-traits.md#global-dictionary-storage-shared-namespace) for details.

Each branch is a fully independent `LedgerState` with its own snapshot, novelty layer, commit chain, storage namespace, and `t` sequence.

#### Nameservice Metadata

When a branch is created, the nameservice records the **source branch name** on the new branch's `NsRecord` (e.g., `source_branch: Some("main")`). The divergence point between the branch and its source is computed on demand by walking the commit chains rather than being stored as a static snapshot.

This metadata enables the system to reconstruct the `BranchedContentStore` tree when loading a branch. For nested branches, the ancestry chain is walked recursively via `source_branch` lookups.

#### API

**Rust:**
```rust
// Create a branch from main (default)
let record = fluree.create_branch("mydb", "dev", None).await?;

// Create a branch from another branch
let record = fluree.create_branch("mydb", "feature", Some("dev")).await?;

// List all branches
let branches = fluree.list_branches("mydb").await?;
```

**HTTP:**
```bash
# Create branch
curl -X POST http://localhost:8090/v1/fluree/branch \
  -H "Content-Type: application/json" \
  -d '{"ledger": "mydb", "branch": "dev"}'

# List branches
curl http://localhost:8090/v1/fluree/branch/mydb
```

**CLI:**
```bash
# Create branch
fluree branch create dev --ledger mydb

# Create branch from another branch
fluree branch create feature-x --from dev --ledger mydb

# List branches
fluree branch list --ledger mydb
```

#### Dropping a Branch

Branches can be deleted with `drop_branch`. The `main` branch cannot be dropped.

Branches use **reference counting** (`branches` field on `NsRecord`) to track child branches. This enables safe deletion:

- **Leaf branch** (no children, `branches == 0`): Fully dropped — storage artifacts are deleted, the NsRecord is purged, and the parent's child count is decremented. If the parent was previously retracted and its count reaches 0, it is cascade-dropped.
- **Branch with children** (`branches > 0`): Retracted (hidden from listings, transactions rejected) but storage is preserved so children can still read parent data via `BranchedContentStore` fallback. When the last child is dropped and the count reaches 0, the retracted branch is automatically cascade-purged.

**Rust API:**
```rust
// Drop a leaf branch
let report = fluree.drop_branch("mydb", "dev").await?;

// report.deferred == false for leaf branches
// report.deferred == true for branches with children
// report.cascaded contains any ancestor branches that were cascade-dropped
```

**HTTP API:**
```bash
curl -X POST http://localhost:8090/v1/fluree/drop-branch \
  -H "Content-Type: application/json" \
  -d '{"ledger": "mydb", "branch": "dev"}'
```

**CLI:**
```bash
fluree branch drop dev --ledger mydb
```

See [POST /branch](../api/endpoints.md#post-branch), [GET /branch/{ledger-name}](../api/endpoints.md#get-branchledger-name), and [POST /drop-branch](../api/endpoints.md#post-drop-branch) for full endpoint details.

### Rebasing a Branch

After a branch diverges from its source, you can **rebase** it to replay its unique commits on top of the source branch's current HEAD. This brings the branch up to date with upstream changes without merging.

Rebase detects conflicts when both the branch and source have modified the same (subject, predicate, graph) tuples. Five conflict resolution strategies are available:

| Strategy | Behavior |
|----------|----------|
| `take-both` (default) | Replay as-is, both values coexist (multi-cardinality) |
| `abort` | Fail on first conflict, no changes applied |
| `take-source` | Drop branch's conflicting flakes (source wins) |
| `take-branch` | Keep branch's flakes, retract source's conflicting values |
| `skip` | Skip entire commit if any flakes conflict |

If the branch has no unique commits, rebase performs a **fast-forward**: it simply updates the branch point to the source's current HEAD without replaying anything.

**Rust API:**
```rust
use fluree_db_api::ConflictStrategy;

let report = fluree.rebase_branch("mydb", "dev", ConflictStrategy::TakeBoth).await?;
// report.replayed — number of commits successfully replayed
// report.conflicts — conflicts detected and resolved
// report.fast_forward — true if no branch commits to replay
```

**HTTP API:**
```bash
curl -X POST http://localhost:8090/v1/fluree/rebase \
  -H "Content-Type: application/json" \
  -d '{"ledger": "mydb", "branch": "dev", "strategy": "take-both"}'
```

**CLI:**
```bash
fluree branch rebase dev --ledger mydb --strategy take-both
```

See [POST /rebase](../api/endpoints.md#post-rebase) for full endpoint details.

## Architecture Deep Dive

### Ledger State Composition

Each ledger combines two layers for query execution:

#### 1. Indexed Database

- **What**: Persisted, optimized snapshot of ledger state
- **When**: Created by background indexing processes
- **Storage**: Large, read-optimized data structures
- **Query Performance**: Fast, efficient for historical queries
- **Update Frequency**: Asynchronous, may lag behind commits

#### 2. Novelty Overlay

- **What**: In-memory representation of uncommitted transactions
- **When**: Transactions between `index_t` and `commit_t`
- **Storage**: Transaction log entries
- **Query Performance**: Slower, requires transaction replay
- **Update Frequency**: Real-time, always current

**Query Execution Model:**

```text
Query Result = Indexed Database (up to t=index_t) 
             + Novelty Overlay (t=index_t+1 to commit_t)
```

This architecture provides:
- **Fast historical queries**: Use appropriate index snapshot
- **Real-time current queries**: Include latest transactions via novelty
- **Efficient background indexing**: Doesn't block new writes
- **Consistent snapshots**: Each query sees a consistent state

### Concurrency Control

The nameservice ensures consistency through several mechanisms:

#### Ref Publishing

- **Commits**: `RefPublisher` uses compare-and-set semantics on the current head identity plus a monotonic `t` guard
- **Indexes**: `publish_index()` only accepts `new_index_t > existing_index_t`
- **Guarantee**: Writers either advance the head or receive an explicit conflict outcome

#### Optimistic Concurrency

- **CAS Operations**: Storage-backed nameservices use compare-and-swap (ETags)
- **Conflict Handling**: Retry on conflicts (expected under contention)
- **Atomic Updates**: Metadata updates are atomic per ledger

#### Consistency Guarantees

- **Read Consistency**: All readers see the same nameservice state
- **Write Consistency**: Monotonic updates prevent time-travel inconsistencies
- **Eventual Consistency**: In distributed deployments, updates propagate eventually

### Distributed Coordination

The nameservice enables coordination across distributed deployments:

#### Multi-Process Coordination

- **Shared State**: Nameservice provides shared view of ledger state
- **Process Discovery**: Processes can discover ledgers created by other processes
- **State Synchronization**: Commit/index state visible to all processes

#### Geographic Distribution

- **Storage Backends**: S3/DynamoDB enable cross-region coordination
- **Replication**: Storage backends handle replication
- **Consistency**: Eventual consistency with monotonic guarantees

#### Scalability Patterns

- **Horizontal Scaling**: Multiple Fluree instances can share nameservice
- **Load Distribution**: Queries can be distributed across instances
- **Storage Distribution**: Ledger data can be stored across multiple backends

### Nameservice Record Lifecycle

Understanding how records evolve:

```text
1. Initialization
   - publish_ledger_init("mydb:main")
   - Creates record with commit_t=0, index_t=0

2. First Transaction
   - Transaction committed at t=1
   - Commit head advanced via `RefPublisher` CAS to `(commit_cid_1, 1)`
   - Record: commit_t=1, index_t=0

3. Indexing Completes
   - Index created for t=1
   - publish_index("mydb:main", index_cid_1, 1)
   - Record: commit_t=1, index_t=1

4. More Transactions
   - Transactions at t=2, t=3, t=4
   - Commit head advanced via CAS for each
   - Record: commit_t=4, index_t=1 (novelty: t=2,3,4)

5. Next Index
   - Index created for t=4
   - publish_index("mydb:main", index_cid_2, 4)
   - Record: commit_t=4, index_t=4 (no novelty)
```

## Best Practices

### Ledger Naming

1. **Use Descriptive Names**: Choose names that clearly indicate purpose
   - Good: `customers:main`, `inventory:prod`, `analytics:warehouse`
   - Bad: `db1:main`, `test:main`, `data:main`

2. **Hierarchical Organization**: Use slashes for logical grouping
   - Good: `tenant/app:main`, `tenant/app:dev`
   - Good: `department/project:branch`

3. **Branch Naming Conventions**: Establish consistent branch naming
   - Good: `feature/authentication`, `bugfix/login-error`
   - Good: `release/v1.2.0`, `hotfix/security-patch`

### Nameservice Configuration

1. **Choose Appropriate Backend**: Match backend to deployment needs
   - Development: File system
   - Single server: File system
   - Distributed/Cloud: S3/DynamoDB

2. **Monitor Novelty Layer**: Track gap between commits and indexes
   - Large gaps indicate indexing lag
   - May need to tune indexing frequency or resources

3. **Handle Retraction Carefully**: Retracted ledgers preserve storage artifacts
   - Use for soft deletes, not hard deletes
   - Do not assume normal query/load APIs will open a retracted ledger; administrative recovery may require nameservice repair or purge

### Performance Considerations

1. **Index Frequency**: Balance indexing frequency with query needs
   - More frequent indexing: Better query performance, more storage
   - Less frequent indexing: Lower overhead, larger novelty layer

2. **Query Patterns**: Understand your query patterns
   - Historical queries: Benefit from frequent indexing
   - Current-only queries: Can tolerate larger novelty layer

3. **Storage Planning**: Plan for index storage growth
   - Each index is a complete snapshot
   - Historical indexes accumulate over time
   - Consider retention policies for old indexes

### Operational Guidelines

1. **Monitor Nameservice Health**: Track nameservice operations
   - Lookup latency
   - Publish success rates
   - Storage backend health

2. **Backup Strategy**: Include nameservice in backup plans
   - File-based: Backup `ns@v2/` directory
   - Storage-based: Use backend backup mechanisms

3. **Error Handling**: Handle nameservice errors gracefully
   - Lookup failures: May indicate ledger doesn't exist
   - Publish failures: May indicate contention (retry)
   - Storage errors: May indicate backend issues

## Troubleshooting

### Ledger Not Found

**Symptom**: Query fails with "ledger not found"

**Possible Causes:**
- Ledger ID misspelled
- Ledger not yet created (no transactions yet)
- Ledger retracted
- Nameservice backend misconfigured

**Solutions:**
- Verify ledger ID spelling and format
- Check if ledger exists: `nameservice.lookup(ledger_id)`
- Verify nameservice backend configuration
- Check ledger status (retracted?)

### Stale Query Results

**Symptom**: Queries don't see latest transactions

**Possible Causes:**
- Novelty layer not being applied
- Index lagging significantly behind commits
- Query caching issues

**Solutions:**
- Check `commit_t` vs `index_t` gap
- Verify indexing process is running
- Check query execution logs
- Consider forcing index update

### Nameservice Contention

**Symptom**: Publish operations failing with conflicts

**Possible Causes:**
- Multiple processes updating same ledger
- High transaction rate
- Storage backend throttling

**Solutions:**
- Implement retry logic with backoff
- Reduce transaction rate if possible
- Scale storage backend (if S3/DynamoDB)
- Check for process coordination issues

This foundation of ledgers and the nameservice enables Fluree's distributed, temporal graph database capabilities, providing the coordination layer needed for scalable, consistent data management.

**Differentiator**: Fluree's nameservice architecture enables true distributed deployments with coordination across multiple processes and machines, unlike single-instance databases. The separation of commits and indexes, combined with the novelty layer, enables real-time queries while maintaining efficient background indexing—a unique architectural advantage.
