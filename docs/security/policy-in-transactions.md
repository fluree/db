# Policy in Transactions

Policies are enforced during transaction processing, validating that users have permission to write data. This document explains how policies affect transaction operations.

## Transaction-Time Authorization

When a transaction is submitted, Fluree:
1. Identifies the subject (from signed transaction)
2. Parses the transaction
3. Collects applicable policies
4. Validates each assertion/retraction
5. Rejects transaction if any operation is unauthorized

**Unauthorized transactions are rejected entirely.**

## Basic Example

### Without Policy

Transaction:
```json
{
  "@graph": [
    { "@id": "ex:alice", "schema:age": 31 }
  ]
}
```

Result: Success (no restrictions)

### With Policy

Policy (owner-only transactions):
```json
{
  "f:subject": "?user",
  "f:action": "transact",
  "f:resource": {
    "ex:owner": "?user"
  },
  "f:allow": true
}
```

Transaction from different user:
```json
{
  "@graph": [
    { "@id": "ex:alice", "schema:age": 31 }
  ]
}
```

Result: **REJECTED** (ex:alice not owned by user)

## Authorization Points

Policies check authorization at multiple points:

### 1. Entity Creation

Creating new entities:

Policy:
```json
{
  "f:subject": { "ex:role": "admin" },
  "f:action": "transact",
  "f:resource": { "@type": "ex:User" },
  "f:allow": true
}
```

Only admins can create User entities.

### 2. Property Updates

Updating existing properties:

Policy:
```json
{
  "f:subject": "?user",
  "f:action": "transact",
  "f:resource": {
    "@id": "?entity",
    "ex:owner": "?user"
  },
  "f:allow": true
}
```

Users can only update entities they own.

### 3. Property Addition

Adding new properties:

Policy:
```json
{
  "f:subject": "*",
  "f:action": "transact",
  "f:resource": {
    "f:predicate": "ex:verified"
  },
  "f:allow": false
}
```

Nobody can set "verified" flag (except admins via separate policy).

### 4. Retractions

Removing data:

Policy:
```json
{
  "f:subject": { "ex:role": "admin" },
  "f:action": "transact",
  "f:operation": "retract",
  "f:allow": true
}
```

Only admins can retract data.

## Transaction Validation

### Per-Triple Validation

Each triple is validated independently:

Transaction:
```json
{
  "@graph": [
    { "@id": "ex:doc1", "schema:title": "Public Doc" },
    { "@id": "ex:doc2", "schema:title": "Private Doc" }
  ]
}
```

If user can create ex:doc1 but not ex:doc2, entire transaction rejected.

### Atomic Transactions

Transactions are atomic:
- All operations must be authorized
- One unauthorized operation = entire transaction rejected
- No partial commits

## Operation Types

### Insert Operations

Policy for inserts:

```json
{
  "f:subject": "*",
  "f:action": "transact",
  "f:operation": "assert",
  "f:resource": { "@type": "ex:PublicData" },
  "f:allow": true
}
```

Anyone can insert public data.

### Update Operations

Policy for updates (retract + assert):

```json
{
  "f:subject": "?user",
  "f:action": "transact",
  "f:operation": ["retract", "assert"],
  "f:resource": {
    "ex:author": "?user"
  },
  "f:allow": true
}
```

Users can update data they authored.

### Delete Operations

Policy for retractions:

```json
{
  "f:subject": { "ex:role": "moderator" },
  "f:action": "transact",
  "f:operation": "retract",
  "f:allow": true
}
```

Only moderators can delete data.

## Property-Level Authorization

### Restricting Specific Properties

Prevent transactions that modify sensitive properties:

```json
{
  "f:subject": "*",
  "f:action": "transact",
  "f:resource": {
    "f:predicate": "ex:balance"
  },
  "f:allow": false
}
```

Nobody can directly modify balance (must use specific API).

### Whitelist Approach

Allow only specific properties:

```json
{
  "f:subject": "?user",
  "f:action": "transact",
  "f:resource": {
    "@id": "?user",
    "f:predicate": ["schema:name", "schema:email", "schema:telephone"]
  },
  "f:allow": true
}
```

Users can only update their name, email, and phone.

## Entity-Level Authorization

### Owner-Based Access

```json
{
  "f:subject": "?user",
  "f:action": "transact",
  "f:resource": {
    "@id": "?entity",
    "ex:owner": "?user"
  },
  "f:allow": true
}
```

### Creator Rights

```json
{
  "f:subject": "?user",
  "f:action": "transact",
  "f:resource": {
    "@id": "?entity",
    "ex:createdBy": "?user"
  },
  "f:allow": true
}
```

### Hierarchical Permissions

```json
{
  "f:subject": "?manager",
  "f:action": "transact",
  "f:resource": {
    "ex:reportsTo": "?manager"
  },
  "f:allow": true
}
```

Managers can modify records of their reports.

## Conditional Authorization

### Status-Based

```json
{
  "f:subject": "?user",
  "f:action": "transact",
  "f:resource": {
    "@id": "?doc",
    "ex:status": "draft"
  },
  "f:condition": [
    { "@id": "?doc", "ex:author": "?user" }
  ],
  "f:allow": true
}
```

Authors can modify documents only while in draft status.

### Time-Based

```json
{
  "f:subject": "?user",
  "f:action": "transact",
  "f:resource": {
    "ex:submittedAt": "?submitTime"
  },
  "f:condition": [
    { "f:filter": "NOW() - ?submitTime < 3600" }
  ],
  "f:allow": true
}
```

Can modify submission within 1 hour.

### Value-Based

```json
{
  "f:subject": "?user",
  "f:action": "transact",
  "f:resource": {
    "ex:amount": "?amount"
  },
  "f:condition": [
    { "f:filter": "?amount <= 1000" },
    { "@id": "?user", "ex:approvalLimit": "?limit" },
    { "f:filter": "?amount <= ?limit" }
  ],
  "f:allow": true
}
```

Users can approve transactions up to their limit.

## Upsert

Policy evaluation with upsert:

```bash
POST /upsert?ledger=mydb:main
```

Fluree checks:
1. Permission to retract existing triples
2. Permission to assert new triples
3. Both must be authorized

Policy:
```json
{
  "f:subject": "?user",
  "f:action": "transact",
  "f:operation": ["retract", "assert"],
  "f:resource": {
    "ex:owner": "?user"
  },
  "f:allow": true
}
```

## WHERE/DELETE/INSERT Updates

Policy evaluation for updates:

Transaction:
```json
{
  "where": [
    { "@id": "ex:alice", "schema:age": "?oldAge" }
  ],
  "delete": [
    { "@id": "ex:alice", "schema:age": "?oldAge" }
  ],
  "insert": [
    { "@id": "ex:alice", "schema:age": 32 }
  ]
}
```

Fluree checks:
1. Permission to query (WHERE clause)
2. Permission to retract (DELETE clause)
3. Permission to assert (INSERT clause)

## Error Responses

### Unauthorized Transaction

```json
{
  "error": "Forbidden",
  "message": "Policy denies transact on ex:alice",
  "code": "POLICY_DENIED",
  "details": {
    "subject": "did:key:z6Mkh...",
    "action": "transact",
    "resource": "ex:alice",
    "policy_evaluated": [
      {
        "id": "ex:owner-policy",
        "matched": true,
        "condition_met": false,
        "decision": "deny"
      }
    ]
  }
}
```

### Property Not Allowed

```json
{
  "error": "Forbidden",
  "message": "Not authorized to modify property ex:verified",
  "code": "PROPERTY_DENIED",
  "details": {
    "subject": "did:key:z6Mkh...",
    "entity": "ex:alice",
    "predicate": "ex:verified",
    "operation": "assert"
  }
}
```

## Signed Transactions

Link transaction to identity:

```javascript
const transaction = {
  "@graph": [
    { "@id": "ex:alice", "schema:name": "Alice" }
  ]
};

const signedTxn = await signTransaction(transaction, privateKey);

await fetch('http://localhost:8090/v1/fluree/upsert?ledger=mydb:main', {
  method: 'POST',
  headers: { 'Content-Type': 'application/jose' },
  body: signedTxn
});
```

Policy uses signer's DID for authorization.

## Provenance Tracking

Policy can enforce provenance:

```json
{
  "f:subject": "?user",
  "f:action": "transact",
  "f:resource": {
    "@id": "?entity"
  },
  "f:condition": [
    { "@id": "?entity", "ex:createdBy": "?user" }
  ],
  "f:allow": true,
  "f:augment": [
    { "@id": "?entity", "ex:modifiedBy": "?user" },
    { "@id": "?entity", "ex:modifiedAt": "NOW()" }
  ]
}
```

Automatically adds modification metadata.

## Common Patterns

### Create Own, Edit Own

```json
[
  {
    "@id": "ex:create-policy",
    "f:subject": "*",
    "f:action": "transact",
    "f:operation": "assert",
    "f:resource": { "@type": "ex:Document" },
    "f:allow": true,
    "f:augment": [
      { "@id": "?newEntity", "ex:owner": "?subject" }
    ]
  },
  {
    "@id": "ex:edit-own-policy",
    "f:subject": "?user",
    "f:action": "transact",
    "f:resource": {
      "ex:owner": "?user"
    },
    "f:allow": true
  }
]
```

### Approval Workflow

```json
[
  {
    "@id": "ex:submit-policy",
    "f:subject": "*",
    "f:action": "transact",
    "f:resource": {
      "@type": "ex:Request",
      "ex:status": "pending"
    },
    "f:allow": true
  },
  {
    "@id": "ex:approve-policy",
    "f:subject": { "ex:role": "approver" },
    "f:action": "transact",
    "f:resource": {
      "@type": "ex:Request",
      "ex:status": "approved"
    },
    "f:allow": true
  }
]
```

### Immutable Records

```json
{
  "f:subject": "*",
  "f:action": "transact",
  "f:operation": "retract",
  "f:resource": { "@type": "ex:AuditLog" },
  "f:allow": false
}
```

Audit logs cannot be modified or deleted.

## Debugging Transaction Policies

### Policy Trace

```bash
curl -X POST "http://localhost:8090/v1/fluree/update?ledger=mydb:main" \
  -H "X-Fluree-Policy-Trace: true" \
  -d '{...}'
```

Response (on error):
```json
{
  "error": "Forbidden",
  "message": "Policy denied transaction",
  "policy_trace": [
    {
      "triple": ["ex:alice", "schema:age", 32],
      "operation": "assert",
      "policies_evaluated": [
        {
          "id": "ex:owner-policy",
          "matched": true,
          "decision": "deny",
          "reason": "ownership condition not met"
        }
      ]
    }
  ]
}
```

### Dry Run

Test transaction without committing:

```bash
curl -X POST "http://localhost:8090/v1/fluree/update?ledger=mydb:main&dryRun=true" \
  -d '{...}'
```

Returns success/failure without actually committing.

## Performance Considerations

### Policy Evaluation Overhead

Transaction validation overhead:
- Type-based policies: Minimal overhead
- Property-based policies: Low overhead
- Complex condition policies: Higher overhead

### Batch Transactions

Policies evaluated per-triple:
- Large transactions take longer to validate
- Consider batch size vs validation time

### Policy Caching

Fluree caches compiled policies:
- First evaluation: Compiles policy
- Subsequent: Uses cached version
- Restart clears cache

## Best Practices

### 1. Default Deny for Writes

```json
{
  "f:subject": "*",
  "f:action": "transact",
  "f:allow": false,
  "f:priority": -1000
}
```

### 2. Separate Create/Update Policies

```json
[
  {
    "@id": "ex:create-policy",
    "f:operation": "assert",
    ...
  },
  {
    "@id": "ex:update-policy",
    "f:operation": ["retract", "assert"],
    ...
  }
]
```

### 3. Validate Business Rules

```json
{
  "f:resource": {
    "ex:price": "?price"
  },
  "f:condition": [
    { "f:filter": "?price > 0" }
  ],
  "f:allow": true
}
```

### 4. Audit Trail

```json
{
  "f:augment": [
    { "@id": "?entity", "ex:lastModifiedBy": "?subject" },
    { "@id": "?entity", "ex:lastModifiedAt": "NOW()" }
  ]
}
```

### 5. Test Transaction Policies

```javascript
async function testTransactionPolicy() {
  const txn = { "@graph": [...] };
  
  try {
    await transact(txn, { subject: "user1" });
    console.log("✓ Authorized");
  } catch (err) {
    if (err.code === "POLICY_DENIED") {
      console.log("✓ Correctly denied");
    } else {
      throw err;
    }
  }
}
```

## Testing Policies from the CLI

The same `--as`, `--policy-class`, and `--default-allow` flags used on
`fluree query` are available on `fluree insert`, `fluree upsert`, and
`fluree update` so you can verify write-time enforcement without any client
code:

```bash
# Attempt a write as an identity that lacks the f:modify policy — expect failure
fluree insert --as did:key:z6MkReadOnly... -f new-data.ttl

# Same write as an authorized identity — expect success
fluree insert --as did:key:z6MkWriter... -f new-data.ttl
```

The flags work locally and against remote servers. On remote, the CLI sends
the policy options as HTTP headers (`fluree-identity`,
`fluree-policy-class`, `fluree-default-allow`) and, for JSON-LD bodies, also
injects them into `opts`. The server applies the **root-impersonation gate**:
your bearer identity may delegate to `--as <iri>` only when the bearer
identity itself has no `f:policyClass` on the target ledger. Restricted
bearers have `--as` force-overridden back to their own identity (and see only
what their own policies permit).

This is the standard service-account pattern — see
[Policy in Queries → Remote impersonation](policy-in-queries.md#remote-impersonation-how-its-authorized)
for the full authorization rules and audit-log format.

### Policy enforcement on transactions is now end-to-end

Prior to this revision, unsigned bearer-authenticated transactions ran under a
root policy bypass. They now build a `PolicyContext` from the (post-header-
merge) opts and route through the policy-enforcing `transact_tracked_with_policy`
path. Practically: a non-root bearer's `f:modify` constraints now apply to
their writes, matching the long-standing query-side behavior. SPARQL UPDATE
inherits the same enforcement, with identity sourced from either the bearer
or the `fluree-identity` header (impersonation-gated).

## Related Documentation

- [Policy Model](policy-model.md) - Policy structure
- [Policy in Queries](policy-in-queries.md) - Read-time enforcement
- [Signed Transactions](../transactions/signed-transactions.md) - Transaction signing
- [Transaction Overview](../transactions/overview.md) - Transaction lifecycle
