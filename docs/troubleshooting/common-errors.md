# Common Errors

This document provides solutions for the most frequently encountered Fluree errors.

## LEDGER_NOT_FOUND

```json
{
  "error": "NotFound",
  "message": "Ledger not found: mydb:main",
  "code": "LEDGER_NOT_FOUND"
}
```

### Causes

1. Ledger doesn't exist
2. Typo in ledger name
3. Wrong branch name
4. Nameservice not initialized

### Solutions

**Check ledger exists:**
```bash
curl http://localhost:8090/v1/fluree/ledgers
```

**Create ledger:**
```bash
curl -X POST "http://localhost:8090/v1/fluree/create" \
  -H "Content-Type: application/json" \
  -d '{"ledger": "mydb:main"}'
```

**Verify spelling:**
- Check for typos in ledger name
- Verify branch name (default is `main`)
- Check case sensitivity

## PARSE_ERROR

```json
{
  "error": "ParseError",
  "message": "Invalid JSON-LD: unexpected token at line 5",
  "code": "PARSE_ERROR",
  "details": {
    "line": 5,
    "column": 12
  }
}
```

### Causes

1. Invalid JSON syntax
2. Invalid JSON-LD structure
3. Invalid SPARQL syntax
4. Missing required fields

### Solutions

**Validate JSON:**
```bash
# Use jq to validate
cat query.json | jq .
```

**Check JSON-LD:**
- Validate @context format
- Check @id and @type values
- Verify array vs object usage

**Check SPARQL:**
- Validate syntax online
- Check PREFIX declarations
- Verify quote matching

**Common JSON Mistakes:**
```json
// Bad: trailing comma
{
  "select": ["?name"],
  "where": [...],
}

// Good: no trailing comma
{
  "select": ["?name"],
  "where": [...]
}
```

## INVALID_IRI

```json
{
  "error": "ValidationError",
  "message": "Invalid IRI: not a valid URI",
  "code": "INVALID_IRI",
  "details": {
    "iri": "not a uri"
  }
}
```

### Causes

1. Malformed IRI
2. Missing namespace prefix
3. Invalid characters
4. Spaces in IRI

### Solutions

**Use valid IRIs:**
```json
// Good
{"@id": "http://example.org/alice"}
{"@id": "ex:alice"}

// Bad
{"@id": "not a uri"}
{"@id": "alice"}  // Missing namespace
{"@id": "ex:alice smith"}  // Space
```

**Define namespace:**
```json
{
  "@context": {
    "ex": "http://example.org/ns/"
  },
  "@graph": [
    {"@id": "ex:alice"}  // Now valid
  ]
}
```

**URL encode spaces:**
```json
{"@id": "ex:alice%20smith"}
```

## UNRESOLVED_COMPACT_IRI

```text
Unresolved compact IRI 'ex:Person': prefix 'ex' is not defined in @context.
If this is intended as an absolute IRI, use a full form (e.g. http://...)
or add the prefix to @context.
```

This error fires from the JSON-LD strict compact-IRI guard. A value that *looks* like a compact IRI (`prefix:suffix`) appeared in an IRI position, but `prefix` is not defined in `@context` and is not a recognised absolute scheme.

### Causes

1. Forgotten `@context` on a query or transaction
2. Misspelled or missing prefix in `@context`
3. Intentionally using a bare `prefix:suffix` string as an opaque identifier

### Solutions

**Add the missing prefix to @context** (most common fix):
```json
{
  "@context": {"ex": "http://example.org/ns/"},
  "@graph": [{"@id": "ex:alice", "ex:name": "Alice"}]
}
```

**Use a full absolute IRI** instead of the compact form:
```json
{
  "@graph": [
    {"@id": "http://example.org/ns/alice", "http://example.org/ns/name": "Alice"}
  ]
}
```

**Opt out of the guard** for legacy data where bare `prefix:suffix` strings are intentional:
```json
{
  "@context": {"ex": "http://example.org/ns/"},
  "opts": {"strictCompactIri": false},
  "@graph": [{"@id": "legacy:alice", "ex:name": "Alice"}]
}
```

The opt-out applies to both queries and transactions. See [IRIs and @context — Strict Compact-IRI Guard](../concepts/iri-and-context.md#strict-compact-iri-guard) for the full policy.

## QUERY_TIMEOUT

```json
{
  "error": "Timeout",
  "message": "Query execution exceeded timeout of 30000ms",
  "code": "QUERY_TIMEOUT",
  "details": {
    "timeout_ms": 30000,
    "elapsed_ms": 31245
  }
}
```

### Causes

1. Complex query
2. Large result set
3. High indexing lag
4. Insufficient resources

### Solutions

**Add LIMIT:**
```json
{
  "select": ["?name"],
  "where": [...],
  "limit": 100  // Add limit
}
```

**Add filters:**
```json
{
  "where": [...],
  "filter": "?age > 18"  // Reduce result set
}
```

**Check indexing lag:**
```bash
curl http://localhost:8090/v1/fluree/info/mydb:main
# If (t - index.t) is large, wait for indexing (or reduce write rate)
```

**Simplify query:**
- Break into smaller queries
- Remove unnecessary joins
- Use more specific patterns

**Increase timeout:**
```bash
curl -X POST http://localhost:8090/v1/fluree/query \
  -H "X-Fluree-Timeout: 60000" \
  -d '{...}'
```

## POLICY_DENIED

```json
{
  "error": "Forbidden",
  "message": "Policy denies access to ledger mydb:main",
  "code": "POLICY_DENIED",
  "details": {
    "subject": "did:key:z6Mkh...",
    "action": "query",
    "ledger": "mydb:main"
  }
}
```

### Causes

1. No permission for operation
2. Missing authentication
3. Policy misconfiguration
4. Wrong DID/identity

### Solutions

**Check authentication:**
```bash
# Are you sending credentials?
curl -H "Authorization: Bearer token" ...
```

**Verify policy:**
```sparql
# Query policies
SELECT ?policy ?subject ?action ?allow
WHERE {
  ?policy a f:Policy .
  ?policy f:subject ?subject .
  ?policy f:action ?action .
  ?policy f:allow ?allow .
}
```

**Test with policy trace:**
```bash
curl -X POST http://localhost:8090/v1/fluree/query \
  -H "X-Fluree-Policy-Trace: true" \
  -d '{...}'
```

**Check DID:**
- Verify DID in signed request
- Check DID is registered
- Verify public key

## TYPE_ERROR

```json
{
  "error": "TypeError",
  "message": "Expected integer, got string",
  "code": "TYPE_ERROR",
  "details": {
    "expected": "xsd:integer",
    "actual": "xsd:string",
    "value": "not a number"
  }
}
```

### Causes

1. Wrong datatype
2. Type mismatch in comparison
3. Invalid type conversion

### Solutions

**Use correct types:**
```json
// Good
{"ex:age": 30}
{"ex:age": {"@value": "30", "@type": "xsd:integer"}}

// Bad
{"ex:age": "30"}  // String, not integer
```

**Check type constraints:**
- Verify expected types
- Use explicit @type
- Validate before submitting

## PAYLOAD_TOO_LARGE

```json
{
  "error": "PayloadTooLarge",
  "message": "Transaction exceeds maximum size of 10485760 bytes",
  "code": "PAYLOAD_TOO_LARGE",
  "details": {
    "max_size": 10485760,
    "actual_size": 15000000
  }
}
```

### Causes

1. Transaction too large
2. Query result too large
3. Large embedded data

### Solutions

**Batch large transactions:**
```javascript
const batchSize = 1000;
for (let i = 0; i < entities.length; i += batchSize) {
  const batch = entities.slice(i, i + batchSize);
  await transact({"@graph": batch});
}
```

**Use LIMIT for queries:**
```json
{
  "select": ["?name"],
  "where": [...],
  "limit": 1000  // Paginate
}
```

**Increase limits (if appropriate):**
```bash
./fluree-db-server --max-transaction-size 20971520
```

## STORAGE_ERROR

```json
{
  "error": "StorageError",
  "message": "Cannot write to storage",
  "code": "STORAGE_ERROR"
}
```

### Causes

1. Disk full (file storage)
2. Permission errors
3. AWS connectivity (AWS storage)
4. Storage backend down

### Solutions

**File Storage:**
```bash
# Check disk space
df -h /var/lib/fluree

# Check permissions
ls -la /var/lib/fluree
sudo chown -R fluree:fluree /var/lib/fluree
```

**AWS Storage:**
```bash
# Check AWS credentials
aws sts get-caller-identity

# Check S3 access
aws s3 ls s3://fluree-prod-data/

# Check DynamoDB
aws dynamodb describe-table --table-name fluree-nameservice
```

## HIGH_INDEXING_LAG

Not an error, but a warning condition.

### Symptoms

```bash
curl http://localhost:8090/v1/fluree/info/mydb:main
```

```json
{
  "commit_t": 150,
  "index_t": 0
}
```

### Causes

1. Transaction rate exceeds indexing capacity
2. Large transactions
3. Insufficient resources
4. Storage bottleneck

### Solutions

**Tune indexing:**
```bash
fluree-server \
  --indexing-enabled \
  --reindex-min-bytes 100000 \
  --reindex-max-bytes 1000000
```

**Reduce transaction rate:**
```javascript
// Add delay between transactions
await transact(data);
await sleep(100);
```

**Wait for catchup:**
```javascript
async function waitForIndexing() {
  while (true) {
    const status = await getStatus();
    const lag = status.commit_t - status.index_t;
    if (lag < 10) break;
    await sleep(1000);
  }
}
```

**Add resources:**
- More CPU
- More memory
- Faster disk

## CONCURRENT_MODIFICATION

```json
{
  "error": "Conflict",
  "message": "Concurrent modification detected",
  "code": "CONCURRENT_MODIFICATION"
}
```

### Causes

1. Multiple processes updating same entity
2. Nameservice contention
3. Race condition

### Solutions

**Implement retry:**
```javascript
async function transactWithRetry(data, maxRetries = 3) {
  for (let i = 0; i < maxRetries; i++) {
    try {
      return await transact(data);
    } catch (err) {
      if (err.code === 'CONCURRENT_MODIFICATION' && i < maxRetries - 1) {
        await sleep(Math.pow(2, i) * 100);
        continue;
      }
      throw err;
    }
  }
}
```

**Use upsert for retry-friendly transactions:**
```bash
# Upsert is more retry-friendly for idempotent entity transactions
POST /upsert?ledger=mydb:main
```

## SIGNATURE_VERIFICATION_FAILED

```json
{
  "error": "SignatureVerificationFailed",
  "message": "Invalid signature",
  "code": "INVALID_SIGNATURE"
}
```

### Causes

1. Wrong private key
2. Payload modified after signing
3. Incorrect algorithm
4. Key not registered

### Solutions

**Verify signing process:**
```javascript
// Ensure payload not modified
const payload = JSON.stringify(transaction);
const jws = await sign(payload, privateKey);
// Don't modify payload after signing
```

**Check algorithm:**
```json
{
  "alg": "EdDSA",  // Must match key type
  "kid": "did:key:z6Mkh..."
}
```

**Verify public-key material:** standalone server signed requests use the key
material embedded in supported JWS/JWT headers (or configured OIDC JWKS). There
is no `/admin/keys` registration endpoint.

## Memory Issues

### Symptoms

- Out of memory errors
- Server crashes
- Slow performance
- Swap usage

### Solutions

**Check memory:**
```bash
curl http://localhost:8090/v1/fluree/stats
```

**Reduce memory usage:**
```bash
# See docs/operations/configuration.md for current memory-related flags.
# In general: reduce write/query load, reduce indexing lag, and provision more RAM.
```

**Add more RAM:**
- Upgrade server
- Use cloud instance with more memory

**Reduce novelty:**
- Index more frequently
- Reduce transaction size

## Troubleshooting Checklist

When encountering issues, check:

1. [ ] Server is running
2. [ ] Can connect to server
3. [ ] Health endpoint returns healthy
4. [ ] Logs show no errors
5. [ ] Ledger exists
6. [ ] Correct ledger name/branch
7. [ ] Valid JSON-LD/SPARQL syntax
8. [ ] Sufficient resources (disk, memory)
9. [ ] No network issues
10. [ ] Authentication working (if required)

## Related Documentation

- [Debugging Queries](debugging-queries.md) - Query-specific debugging
- [API Errors](../api/errors.md) - HTTP error reference
- [Operations](../operations/README.md) - Operational guides
- [Telemetry](../operations/telemetry.md) - Monitoring and logging
