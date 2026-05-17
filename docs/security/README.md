# Security and Policy

Fluree provides comprehensive security features including authentication, fine-grained access control through policies, and transparent encryption of data at rest.

## Authentication

### [Authentication](authentication.md)

Fluree's authentication model, covering:
- Identity vs transport (DIDs, signed requests, Bearer tokens)
- Three auth modes: decentralized did:key, standalone server tokens, OIDC/OAuth2
- Bearer token claim set and scope definitions
- Replication vs query access boundary
- Token verification paths (Ed25519 + OIDC/JWKS)

## Data Encryption

### [Storage Encryption](encryption.md)

Protect data at rest with AES-256-GCM encryption:
- Transparent encryption/decryption
- Environment variable key configuration
- Portable ciphertext format
- Key rotation support

## Commit Integrity

### [Commit Signing and Attestation](commit-signing.md)

Cryptographic proof of which node wrote a commit:
- Ed25519 signatures over domain-separated commit digests
- Embedded signature blocks in commit files
- did:key signer identities
- Future: detached attestations and consensus policies

## Policy System

### [Policy Model and Inputs](policy-model.md)

Understanding Fluree's policy architecture:
- Policy structure and syntax
- Subject, action, resource model
- Policy evaluation order
- Input data for policy decisions
- Default allow vs default deny

### [Policy in Queries](policy-in-queries.md)

How policies affect query execution:
- Query-time filtering
- Result set restrictions
- Pattern-based filtering
- Performance considerations
- Policy debugging for queries

### [Policy in Transactions](policy-in-transactions.md)

How policies affect transaction operations:
- Transaction validation
- Authorization checks
- Entity-level permissions
- Property-level permissions
- Policy-based retractions

### [Programmatic Policy API (Rust)](programmatic-policy.md)

Using policies in Rust applications:
- `wrap_identity_policy_view` - Identity-based policy lookup via `f:policyClass`
- `wrap_policy_view` - Inline policies with `QueryConnectionOptions`
- Policy precedence rules
- Transaction-side policy enforcement
- Historical views with policy

### [Cross-ledger governance](cross-ledger-policy.md)

Govern many data ledgers from one model ledger via any of the
five `f:GraphRef`-shaped predicates with `f:ledger`:
- Two-ledger configuration pattern (model M, data D)
- Cross-ledger policy (`f:policySource`): `f:policyClass`
  filtering, `f:AccessPolicy` baseline, engaging the policy
  path over HTTP
- Cross-ledger uniqueness constraints (`f:constraintsSource`):
  tx-time enforcement of M's `f:enforceUnique` annotations on
  D's transactions
- Cross-ledger schema/ontology (`f:schemaSource`): M's RDFS/OWL
  axioms feed D's reasoner (single graph, transitive
  `owl:imports` deferred)
- Cross-ledger SHACL shapes (`f:shapesSource`): M's shape
  definitions compile against D's staged namespace at
  validation time
- Cross-ledger datalog rules (`f:rulesSource`): M's `f:rule`
  JSON bodies feed D's query-time datalog evaluator
- Failure modes and HTTP status mapping
- Cache and update semantics

## Key Concepts

### Data-Level Security

Fluree enforces security at the data level, not just the application level:
- Users see only authorized data
- Policies applied during query execution
- No unauthorized data leakage
- Transparent to applications

### Policy as Data

Policies are stored as RDF triples in the database:
- Version controlled with data
- Query policies like any data
- Time travel for policy history
- Policies can reference other data

### Identity-Based Access

Policies use decentralized identifiers (DIDs):
- did:key for cryptographic identity
- did:web for organization identity
- Signed requests link to DID
- Policies grant/deny based on DID

## Policy Structure

Basic policy format:

```json
{
  "@context": {
    "f": "https://ns.flur.ee/db#",
    "ex": "http://example.org/ns/"
  },
  "@id": "ex:read-policy",
  "@type": "f:Policy",
  "f:subject": "did:key:z6Mkh...",
  "f:action": "query",
  "f:resource": {
    "@type": "schema:Person"
  },
  "f:allow": true
}
```

**Subject:** Who (DID, role, group)
**Action:** What operation (query, transact)
**Resource:** Which data (type, predicate, specific entities)
**Allow/Deny:** Grant or deny access

## Policy Enforcement Points

### Query Time

Policies filter query results:

```sparql
SELECT ?name
WHERE {
  ?person schema:name ?name .
}
```

Policy filters results to only show authorized people.

### Transaction Time

Policies validate transaction operations:

```json
{
  "@graph": [
    { "@id": "ex:alice", "schema:age": 31 }
  ]
}
```

Policy checks if user can modify ex:alice.

## Common Policy Patterns

### Allow All (Development)

```json
{
  "@id": "ex:allow-all",
  "f:subject": "*",
  "f:action": "*",
  "f:allow": true
}
```

### Role-Based Access

```json
{
  "@id": "ex:admin-policy",
  "f:subject": { "ex:role": "admin" },
  "f:action": "*",
  "f:allow": true
}
```

### Resource-Type Based

```json
{
  "@id": "ex:public-data-policy",
  "f:subject": "*",
  "f:action": "query",
  "f:resource": { "@type": "ex:PublicData" },
  "f:allow": true
}
```

### Property-Level Access

```json
{
  "@id": "ex:sensitive-property-policy",
  "f:subject": { "ex:role": "hr" },
  "f:action": "query",
  "f:resource": {
    "f:predicate": "ex:salary"
  },
  "f:allow": true
}
```

### Owner-Based Access

```json
{
  "@id": "ex:owner-policy",
  "f:subject": "?user",
  "f:action": ["query", "transact"],
  "f:resource": {
    "ex:owner": "?user"
  },
  "f:allow": true
}
```

## Policy Evaluation

### Evaluation Order

1. **Collect applicable policies** based on subject, action, resource
2. **Evaluate each policy** against request context
3. **Combine results** using policy combining algorithm
4. **Apply default** if no policies match

### Combining Algorithms

**Deny Overrides** (default):
- If any policy denies, access denied
- Otherwise, allow if any policy allows
- Default: deny if no matches

**Allow Overrides:**
- If any policy allows, access granted
- Otherwise, deny if any policy denies
- Default: deny if no matches

## Policy Context

Policies have access to runtime context:

**Request Context:**
- Subject DID
- Action being performed
- Target resource/entity
- Timestamp

**Data Context:**
- Entity properties
- Related entities
- Graph structure
- Historical data

**Example using context:**
```json
{
  "f:subject": "?user",
  "f:resource": {
    "ex:department": "?dept"
  },
  "f:condition": "?user ex:department ?dept",
  "f:allow": true
}
```

Allows access if user is in same department as resource.

## Multi-Tenant Policies

Isolate data by tenant:

```json
{
  "@id": "ex:tenant-isolation-policy",
  "f:subject": "?user",
  "f:action": "*",
  "f:resource": {
    "ex:tenant": "?tenant"
  },
  "f:condition": "?user ex:tenant ?tenant",
  "f:allow": true
}
```

Users can only access data from their tenant.

## Policy Performance

### Efficient Policies

Good (specific):
```json
{
  "f:resource": { "@type": "ex:PublicData" },
  "f:allow": true
}
```

Less efficient (broad):
```json
{
  "f:resource": { "?pred": "?value" },
  "f:condition": "complex graph pattern",
  "f:allow": true
}
```

### Query Optimization

Policies are optimized during query planning:
- Type-based filters pushed down
- Property filters optimized
- Complex patterns may impact performance

## Policy Management

### Creating Policies

Policies are created via transactions:

```bash
curl -X POST "http://localhost:8090/v1/fluree/upsert?ledger=policies:main" \
  -H "Content-Type: application/json" \
  -d '{
    "@graph": [
      {
        "@id": "ex:new-policy",
        "@type": "f:Policy",
        "f:subject": "did:key:z6Mkh...",
        "f:action": "query",
        "f:allow": true
      }
    ]
  }'
```

### Updating Policies

Update using WHERE/DELETE/INSERT:

```json
{
  "where": [
    { "@id": "ex:policy-1", "f:allow": "?oldValue" }
  ],
  "delete": [
    { "@id": "ex:policy-1", "f:allow": "?oldValue" }
  ],
  "insert": [
    { "@id": "ex:policy-1", "f:allow": false }
  ]
}
```

### Policy Versioning

Policies are versioned with data:
- Time travel to see historical policies
- Audit who changed policies when
- Rollback policies if needed

## Security Best Practices

### 1. Principle of Least Privilege

Grant minimum necessary permissions:

```json
// Good: Specific permissions
{
  "f:subject": "did:key:z6Mkh...",
  "f:action": "query",
  "f:resource": { "@type": "ex:PublicData" },
  "f:allow": true
}

// Bad: Overly broad
{
  "f:subject": "did:key:z6Mkh...",
  "f:action": "*",
  "f:allow": true
}
```

### 2. Default Deny

Start with deny-all, add specific allows:

```json
// Default policy
{
  "@id": "ex:default",
  "f:subject": "*",
  "f:action": "*",
  "f:allow": false
}

// Specific allows
{
  "@id": "ex:public-read",
  "f:subject": "*",
  "f:action": "query",
  "f:resource": { "@type": "ex:PublicData" },
  "f:allow": true
}
```

### 3. Use Roles

Define roles, not individual permissions:

```json
{
  "@id": "ex:admin-role",
  "@type": "ex:Role",
  "ex:permissions": ["read", "write", "admin"]
}

{
  "@id": "ex:role-policy",
  "f:subject": { "ex:hasRole": "ex:admin-role" },
  "f:action": "*",
  "f:allow": true
}
```

### 4. Audit Policy Changes

Track who changes policies:

```json
{
  "@id": "ex:policy-audit",
  "ex:changedBy": "did:key:z6Mkh...",
  "ex:changedAt": "2024-01-22T10:00:00Z",
  "ex:reason": "Added read access for contractors"
}
```

### 5. Test Policies

Test policies before deploying:

```javascript
async function testPolicy(policy, testCases) {
  for (const testCase of testCases) {
    const result = await evaluatePolicy(policy, testCase);
    assert.equal(result.allowed, testCase.expected);
  }
}
```

## Related Documentation

- [Verifiable Data](../concepts/verifiable-data.md) - Cryptographic signatures
- [Signed Requests](../api/signed-requests.md) - Request authentication
- [Signed Transactions](../transactions/signed-transactions.md) - Transaction signing
- [Commit Signing and Attestation](commit-signing.md) - Commit-level signatures
