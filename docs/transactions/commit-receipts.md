# Commit Receipts and tx-id

Every successful transaction returns a **commit receipt** containing metadata about the transaction. This receipt provides important information for tracking, auditing, and referencing transactions.

## Commit Receipt Structure

Basic commit receipt:

```json
{
  "t": 42,
  "timestamp": "2024-01-22T10:30:00.000Z",
  "commit_id": "bafybeig...commitT42",
  "flakes_added": 15,
  "flakes_retracted": 3,
  "previous_commit_id": "bafybeig...commitT41"
}
```

## Receipt Fields

### Transaction Time (t)

The **transaction time** is a monotonically increasing integer uniquely identifying this transaction:

```json
{
  "t": 42
}
```

**Properties:**
- Unique across all ledgers in the Fluree instance
- Monotonically increasing (never decreases)
- Used for time travel queries
- Basis for temporal ordering

**Usage:**

```bash
# Query at specific transaction
curl -X POST http://localhost:8090/v1/fluree/query \
  -d '{"from": "mydb:main@t:42", ...}'
```

**Read-after-write consistency:** The `t` value is the key to ensuring queries
see freshly committed data. Pass it as `min_t` to `refresh()` to gate queries
on a minimum transaction time. See [Time Travel — Consistency and Read-After-Write](../concepts/time-travel.md#consistency-and-read-after-write) for details.

### Timestamp

ISO 8601 formatted timestamp of when the transaction was committed:

```json
{
  "timestamp": "2024-01-22T10:30:00.000Z"
}
```

**Properties:**
- UTC timezone
- Millisecond precision
- Server-assigned (not client-provided)
- Monotonic (within same transaction time ordering)

**Usage:**

```bash
# Query at specific time
curl -X POST http://localhost:8090/v1/fluree/query \
  -d '{"from": "mydb:main@iso:2024-01-22T10:30:00Z", ...}'
```

### Commit ID

Content-addressed identifier for the commit:

```json
{
  "commit_id": "bafybeig...commitT42"
}
```

**Properties:**
- CIDv1 value (base32-lower multibase string)
- Derived from the commit's canonical bytes via SHA-256
- Storage-agnostic -- does not depend on where the commit is stored
- Can be used to fetch the commit from any content store

**Usage:**

```bash
# Query at specific commit
curl -X POST http://localhost:8090/v1/fluree/query \
  -d '{"from": "mydb:main@commit:bafybeig...commitT42", ...}'
```

### Flake Counts

Number of triples added and retracted:

```json
{
  "flakes_added": 15,
  "flakes_retracted": 3
}
```

**flakes_added:** Number of new triples asserted
**flakes_retracted:** Number of existing triples removed

Net change: `flakes_added - flakes_retracted`

### Previous Commit

ContentId of the previous commit (forms a chain):

```json
{
  "previous_commit_id": "bafybeig...commitT41"
}
```

**Properties:**
- Links to parent commit by ContentId
- Forms immutable commit chain
- Enables commit history traversal
- `null` for first transaction (t=1)

## Extended Receipt Fields

### Author (Signed Transactions)

For signed transactions, includes author DID:

```json
{
  "t": 42,
  "author": "did:key:z6MkhaXgBZDvotDkL5257faiztiGiC2QtKLGpbnnEGta2doK",
  "signature": "z58DAdFfa9SkqZMVP...",
  ...
}
```

### Message

Optional commit message (if provided):

```json
{
  "t": 42,
  "message": "Add new customer records for Q1 2024",
  ...
}
```

### Ledger

Ledger ID:

```json
{
  "t": 42,
  "ledger": "mydb:main",
  ...
}
```

### Duration

Transaction processing time in milliseconds:

```json
{
  "t": 42,
  "duration_ms": 45,
  ...
}
```

## Using Transaction IDs

### Referencing Transactions

Store transaction ID for later reference:

```javascript
const receipt = await transact({
  "@graph": [{ "@id": "ex:alice", "schema:name": "Alice" }]
});

// Store for audit trail
await logTransaction({
  entity: "ex:alice",
  operation: "create",
  transactionId: receipt.t,
  timestamp: receipt.timestamp
});
```

### Historical Queries

Query data at specific transaction:

```javascript
// Get data as it was at transaction 42
const historicalData = await query({
  from: `mydb:main@t:${receipt.t}`,
  select: ["?name"],
  where: [{ "@id": "ex:alice", "schema:name": "?name" }]
});
```

### Commit Verification

Verify commit integrity by re-deriving the ContentId from fetched bytes:

```javascript
async function verifyCommit(receipt) {
  const bytes = await contentStore.get(receipt.commit_id);
  const derivedCid = computeContentId("Commit", bytes);

  if (derivedCid !== receipt.commit_id) {
    throw new Error('Commit integrity violation!');
  }
}
```

## Commit Chain

Commits form an immutable chain:

```text
t=1 (cid:aaa) ← t=2 (cid:bbb) ← t=3 (cid:ccc) ← t=4 (cid:ddd)
  ↑                ↑                ↑                ↑
  |                |                |                |
previous=null   previous=aaa    previous=bbb    previous=ccc
```

### Traversing History

Walk the commit chain:

```javascript
async function getCommitHistory(ledger, fromT, toT) {
  const history = [];
  let currentT = fromT;
  
  while (currentT >= toT) {
    const commit = await getCommit(ledger, currentT);
    history.push(commit);
    currentT = commit.previous_t;
  }
  
  return history;
}
```

## Querying Commit Metadata

### SPARQL Query for Commits

```sparql
PREFIX f: <https://ns.flur.ee/db#>

SELECT ?t ?timestamp ?commitId ?author
WHERE {
  ?commit a f:Commit ;
          f:t ?t ;
          f:timestamp ?timestamp ;
          f:commitId ?commitId .
  OPTIONAL { ?commit f:author ?author }
}
ORDER BY DESC(?t)
LIMIT 10
```

### JSON-LD Query for Recent Commits

```json
{
  "@context": {
    "f": "https://ns.flur.ee/db#"
  },
  "select": ["?t", "?timestamp", "?commitId"],
  "where": [
    { "@id": "?commit", "@type": "f:Commit" },
    { "@id": "?commit", "f:t": "?t" },
    { "@id": "?commit", "f:timestamp": "?timestamp" },
    { "@id": "?commit", "f:commitId": "?commitId" }
  ],
  "orderBy": ["-?t"],
  "limit": 10
}
```

## Receipt Storage

### Application Database

Store receipts in your application database:

```sql
CREATE TABLE transaction_receipts (
  id SERIAL PRIMARY KEY,
  ledger VARCHAR(255),
  transaction_t INTEGER,
  commit_id TEXT,
  timestamp TIMESTAMP,
  flakes_added INTEGER,
  flakes_retracted INTEGER,
  author VARCHAR(255),
  created_at TIMESTAMP DEFAULT NOW()
);
```

### Document Store

Store as JSON documents:

```javascript
await mongodb.collection('receipts').insertOne({
  ledger: receipt.ledger,
  t: receipt.t,
  commit_id: receipt.commit_id,
  timestamp: receipt.timestamp,
  flakes: {
    added: receipt.flakes_added,
    retracted: receipt.flakes_retracted
  },
  metadata: {
    author: receipt.author,
    duration_ms: receipt.duration_ms
  }
});
```

### Time-Series Database

For analytics:

```javascript
await influxdb.writePoint({
  measurement: 'transactions',
  tags: { ledger: receipt.ledger },
  fields: {
    t: receipt.t,
    flakes_added: receipt.flakes_added,
    flakes_retracted: receipt.flakes_retracted,
    duration_ms: receipt.duration_ms
  },
  timestamp: new Date(receipt.timestamp)
});
```

## Audit Trail

### Transaction Log

Build complete audit log from receipts:

```javascript
async function buildAuditLog(ledger, startDate, endDate) {
  const receipts = await fetchReceipts(ledger, startDate, endDate);
  
  return receipts.map(r => ({
    time: r.timestamp,
    transactionId: r.t,
    author: r.author || 'anonymous',
    changes: {
      added: r.flakes_added,
      removed: r.flakes_retracted
    },
    commit: r.commit_id,
    verifiable: true
  }));
}
```

### Compliance Reports

Generate compliance reports:

```javascript
async function generateComplianceReport(ledger, period) {
  const receipts = await fetchReceipts(ledger, period.start, period.end);
  
  return {
    period: period,
    totalTransactions: receipts.length,
    totalChanges: receipts.reduce((sum, r) => sum + r.flakes_added, 0),
    authors: [...new Set(receipts.map(r => r.author))],
    verifiedChain: verifyCommitChain(receipts)
  };
}
```

## Performance Monitoring

### Transaction Metrics

Track transaction performance:

```javascript
function analyzeReceipts(receipts) {
  const durations = receipts.map(r => r.duration_ms);
  const sizes = receipts.map(r => r.flakes_added + r.flakes_retracted);
  
  return {
    avgDuration: average(durations),
    maxDuration: Math.max(...durations),
    avgSize: average(sizes),
    maxSize: Math.max(...sizes),
    throughput: receipts.length / (period.hours)
  };
}
```

### Alert on Anomalies

```javascript
function checkForAnomalies(receipt) {
  if (receipt.duration_ms > 1000) {
    alert(`Slow transaction: ${receipt.t} took ${receipt.duration_ms}ms`);
  }
  
  if (receipt.flakes_added > 10000) {
    alert(`Large transaction: ${receipt.t} added ${receipt.flakes_added} flakes`);
  }
}
```

## Best Practices

### 1. Always Store Receipts

Store transaction receipts for audit trail:

```javascript
const receipt = await transact(transaction);
await storeReceipt(receipt);
```

### 2. Verify Commit Chain

Periodically verify commit chain integrity:

```javascript
async function verifyChainIntegrity(ledger) {
  const receipts = await fetchAllReceipts(ledger);
  
  for (let i = 1; i < receipts.length; i++) {
    if (receipts[i].previous_commit_id !== receipts[i-1].commit_id) {
      throw new Error(`Chain broken at t=${receipts[i].t}`);
    }
  }
}
```

### 3. Use Transaction IDs for References

Store transaction IDs rather than timestamps:

Good:
```javascript
{ entity: "ex:alice", createdAt_t: 42 }
```

Less reliable:
```javascript
{ entity: "ex:alice", createdAt: "2024-01-22T10:30:00Z" }
```

### 4. Monitor Performance

Track receipt metadata for performance insights:

```javascript
const avgDuration = receipts.reduce((sum, r) => sum + r.duration_ms, 0) / receipts.length;
```

### 5. Include in Error Handling

Log receipt info on errors:

```javascript
try {
  const receipt = await transact(transaction);
  logger.info(`Transaction successful: t=${receipt.t}`);
} catch (err) {
  logger.error(`Transaction failed`, {
    error: err.message,
    transaction: transaction
  });
}
```

## Related Documentation

- [Overview](overview.md) - Transaction overview
- [Signed Transactions](signed-transactions.md) - Transaction signing
- [Commit Signing and Attestation](../security/commit-signing.md) - Commit-level signatures
- [Time Travel](../concepts/time-travel.md) - Historical queries
- [Indexing Side-Effects](indexing-side-effects.md) - Indexing behavior
