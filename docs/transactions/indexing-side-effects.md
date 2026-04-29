# Indexing Side-Effects

Transactions in Fluree trigger background indexing processes that build query-optimized data structures. Understanding these side-effects is crucial for performance tuning and capacity planning.

## What is Indexing?

**Indexing** is the process of building query-optimized data structures from transaction data. Fluree maintains four index permutations (SPOT, POST, OPST, PSOT) that enable efficient query execution.

### Commit vs Index

**Commit (immediate):**
- Transaction written to log
- Small, append-only files
- Published to nameservice immediately
- Available for time travel queries

**Index (asynchronous):**
- Query-optimized structures built
- Background process
- Published to nameservice when complete
- May lag behind commits

## Index Structure

Fluree maintains four index permutations:

### SPOT (Subject-Predicate-Object-Time)

```text
ex:alice → schema:name → "Alice" → [t=1, t=5, t=10]
ex:alice → schema:age → 30 → [t=1]
ex:alice → schema:age → 31 → [t=10]
```

Optimized for: "What are all properties of this subject?"

### POST (Predicate-Object-Subject-Time)

```text
schema:name → "Alice" → ex:alice → [t=1, t=5, t=10]
schema:age → 30 → ex:alice → [t=1]
schema:age → 31 → ex:alice → [t=10]
```

Optimized for: "What subjects have this property/value?"

### OPST (Object-Predicate-Subject-Time)

```text
"Alice" → schema:name → ex:alice → [t=1, t=5, t=10]
30 → schema:age → ex:alice → [t=1]
31 → schema:age → ex:alice → [t=10]
```

Optimized for: "What subjects have this value?"

### PSOT (Predicate-Subject-Object-Time)

```text
schema:name → ex:alice → "Alice" → [t=1, t=5, t=10]
schema:age → ex:alice → 30 → [t=1]
schema:age → ex:alice → 31 → [t=10]
```

Optimized for: "What are all values for this predicate?"

## Indexing Pipeline

### 1. Transaction Commit

```text
t=42: Transaction committed
  - Flakes written to transaction log
  - Commit published to nameservice
  - commit_t updated to 42
```

### 2. Index Trigger

Background indexing process detects new commits:
```text
Indexer: commit_t=42, index_t=40
Indexer: Need to index t=41, t=42
```

### 3. Index Building

Process transactions to build indexes:
```text
For each flake in t=41, t=42:
  - Update SPOT index
  - Update POST index
  - Update OPST index
  - Update PSOT index
```

### 4. Index Publication

When complete, publish new index:
```text
  - Write index snapshot to storage
  - Publish index_id to nameservice
  - Update index_t to 42
```

## Novelty Layer

The **novelty layer** is the gap between indexed and committed data:

```text
commit_t = 45
index_t = 40
novelty layer = [t=41, t=42, t=43, t=44, t=45]
```

### Query Execution with Novelty

Queries combine index + novelty:

```text
Query Result = Indexed Data (t ≤ 40) + Novelty Layer (41 ≤ t ≤ 45)
```

**Performance Impact:**
- Small novelty: Fast queries (mostly indexed)
- Large novelty: Slower queries (more transaction replay)

## Indexing Performance

### Transaction Size Impact

Larger transactions take longer to index:

```text
Transaction with 10 flakes:
  - 10 flakes × 4 indexes = 40 index updates
  - Indexing time: ~1ms

Transaction with 10,000 flakes:
  - 10,000 flakes × 4 indexes = 40,000 index updates
  - Indexing time: ~100ms
```

### Indexing Rate

Typical indexing rates:

```text
Light load:
  - 1,000 flakes/second
  - ~10 moderate transactions/second

Heavy load:
  - 10,000 flakes/second
  - ~100 moderate transactions/second
```

Actual rates depend on:
- Hardware (CPU, disk I/O)
- Storage backend (memory, file, AWS)
- Transaction patterns
- System load

## Monitoring Indexing

### Check Indexing Status

```bash
curl http://localhost:8090/v1/fluree/info/mydb:main
```

Response:
```json
{
  "ledger_id": "mydb:main",
  "commit_t": 150,
  "index_t": 140
}
```

**Indexing lag (txns):** `commit_t - index_t` = number of unindexed transactions

### Healthy vs Unhealthy

**Healthy:**
```text
commit_t = 1000
index_t = 998
novelty = 2 transactions (good!)
```

**Unhealthy:**
```text
commit_t = 1000
index_t = 850
novelty = 150 transactions (indexing lag!)
```

## Indexing Lag

**Indexing lag** occurs when indexing can't keep up with transaction rate.

### Causes

1. **High Transaction Rate**
   - More transactions than indexing can handle
   - Sustained write load

2. **Large Transactions**
   - Individual transactions with many flakes
   - Bulk imports

3. **Resource Constraints**
   - CPU bottleneck
   - Disk I/O bottleneck
   - Memory pressure

4. **Storage Backend Latency**
   - Slow storage (network attached)
   - AWS S3 latency

### Impact

Large indexing lag affects:

**Query Performance:**
- More novelty to replay
- Slower query execution
- Higher CPU usage for queries

**Memory Usage:**
- Novelty layer held in memory
- Larger memory footprint

**Backup/Recovery:**
- Larger gap to replay
- Longer recovery times

## Tuning Indexing

Background indexing is controlled primarily by:

- Enabling/disabling background indexing (`--indexing-enabled` / `FLUREE_INDEXING_ENABLED`)
- Novelty thresholds that trigger indexing / apply backpressure (`--reindex-min-bytes`, `--reindex-max-bytes`)

See [Operations: Configuration](../operations/configuration.md#background-indexing) and [Background Indexing](../indexing-and-search/background-indexing.md) for the canonical settings and tuning guidance.

### 4. Dedicated Indexing Process

For high-load deployments, run dedicated indexer:

```bash
# Main server (transact only; background indexing disabled)
fluree-server --indexing-enabled=false

# Indexing server
./fluree-db-indexer --ledgers mydb:main,mydb:dev
```

## Transaction Patterns and Indexing

### Batch Transactions

Good pattern:
```javascript
// Batch into reasonable sizes
const batchSize = 1000;
for (let i = 0; i < entities.length; i += batchSize) {
  const batch = entities.slice(i, i + batchSize);
  await transact({ "@graph": batch });
  
  // Allow indexing time
  if (i % (batchSize * 10) === 0) {
    await sleep(1000);
  }
}
```

Bad pattern:
```javascript
// Single giant transaction
await transact({ "@graph": allEntities });  // 1 million entities!
```

### Continuous Transactions

For continuous transaction load:

```javascript
async function writeWithBackpressure(data) {
  const status = await checkIndexingStatus();
  
  const lag = status.commit_t - status.index_t;
  if (lag > 100) {
    // Too much lag, slow down
    await sleep(1000);
  }
  
  await transact(data);
}
```

### Bulk Imports

For large imports:

```javascript
async function bulkImport(entities) {
  const batchSize = 1000;
  
  for (let i = 0; i < entities.length; i += batchSize) {
    const batch = entities.slice(i, i + batchSize);
    await transact({ "@graph": batch });
    
    // Wait for indexing to catch up every 10 batches
    if ((i / batchSize) % 10 === 0) {
      await waitForIndexing();
    }
    
    console.log(`Imported ${i + batch.length} / ${entities.length}`);
  }
}

async function waitForIndexing() {
  while (true) {
    const status = await checkIndexingStatus();
    const lag = status.commit_t - status.index_t;
    if (lag < 5) break;
    await sleep(1000);
  }
}
```

## Graph Source Indexing

Graph sources have their own indexing processes:

### BM25 Indexing

Full-text search indexes built asynchronously:

```text
t=100: Transaction with new documents
  - Main index updated
  - BM25 indexer triggered
  - Documents added to BM25 index
```

### Vector Search Indexing

Vector embeddings can be indexed separately for approximate nearest-neighbor (ANN) search via HNSW vector indexes (implemented with `usearch`, feature-gated behind the `vector` feature).

Inline similarity functions (`dotProduct`, `cosineSimilarity`, `euclideanDistance`) do **not** require a separate graph-source index; they compute scores directly during query execution.

```text
t=100: Transaction with embeddings
  - Main index updated
  - Vector indexer triggered
  - Vectors added to vector index
```

See [Vector Search](../indexing-and-search/vector-search.md) for details on HNSW vector indexes and query syntax.

## Best Practices

### 1. Monitor Novelty Layer

Track indexing lag:

```javascript
setInterval(async () => {
  const status = await checkIndexingStatus();
  const lag = status.commit_t - status.index_t;
  metrics.gauge('index_lag_txns', lag);
  
  if (lag > 100) {
    logger.warn(`High indexing lag: ${lag} transactions`);
  }
}, 10000);  // Check every 10 seconds
```

### 2. Batch Appropriately

Keep transactions reasonable size:
- Recommended: 100-1000 entities per transaction
- Maximum: 10,000 entities per transaction

### 3. Rate Limiting

Implement rate limiting for heavy write loads:

```javascript
const rateLimiter = new RateLimiter({
  tokensPerInterval: 100,
  interval: "minute"
});

await rateLimiter.removeTokens(1);
await transact(data);
```

### 4. Scheduled Imports

Run large imports during off-hours:

```javascript
if (isOffPeakHours()) {
  await runBulkImport();
} else {
  logger.info('Deferring bulk import to off-peak hours');
}
```

### 5. Alert on Lag

Set up alerts for indexing lag:

```javascript
const lag = status.commit_t - status.index_t;
if (lag > 200) {
  alert('Critical: Indexing lag > 200 transactions');
}
```

### 6. Capacity Planning

Plan capacity based on write load:

```text
Expected load: 10,000 transactions/day
Average size: 100 flakes/transaction
Total: 1,000,000 flakes/day

Indexing capacity needed: ~12 flakes/second
With 4× safety margin: ~50 flakes/second
```

## Troubleshooting

### High indexing lag

**Symptom:** `commit_t - index_t` growing continuously

**Causes:**
- Transaction rate exceeds indexing capacity
- Large transactions
- Resource constraints

**Solutions:**
- Reduce transaction rate
- Split large transactions
- Increase indexing resources
- Tune indexing parameters

### Slow Queries

**Symptom:** Queries slower than expected

**Possible Cause:** Large novelty layer

**Check:**
```bash
curl http://localhost:8090/v1/fluree/info/mydb:main | jq '.t - .index.t'
```

**Solution:** Wait for indexing or reduce write rate

### Index Memory Usage

**Symptom:** High memory usage

**Cause:** Large indexes or large novelty layer

**Solutions:**
- Increase system memory
- Reduce novelty layer
- Compact indexes (if supported)

## Related Documentation

- [Overview](overview.md) - Transaction overview
- [Insert](insert.md) - Adding data
- [Commit Receipts](commit-receipts.md) - Transaction metadata
- [Background Indexing](../indexing-and-search/background-indexing.md) - Indexing configuration
