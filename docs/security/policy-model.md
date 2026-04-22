# Policy Model and Inputs

Fluree's policy system provides fine-grained access control by evaluating policies against requests. This document explains the policy model, structure, and evaluation process.

## Policy Structure

A policy consists of four main components:

```json
{
  "@context": {
    "f": "https://ns.flur.ee/db#",
    "ex": "http://example.org/ns/"
  },
  "@id": "ex:example-policy",
  "@type": "f:Policy",
  "f:subject": "did:key:z6Mkh...",
  "f:action": "query",
  "f:resource": {
    "@type": "schema:Person"
  },
  "f:allow": true
}
```

### 1. Subject (Who)

Specifies who the policy applies to:

**Specific DID:**
```json
{
  "f:subject": "did:key:z6MkhaXgBZDvotDkL5257faiztiGiC2QtKLGpbnnEGta2doK"
}
```

**Any Subject (wildcard):**
```json
{
  "f:subject": "*"
}
```

**Variable (for conditions):**
```json
{
  "f:subject": "?user"
}
```

**Role-Based:**
```json
{
  "f:subject": {
    "ex:role": "admin"
  }
}
```

**Group-Based:**
```json
{
  "f:subject": {
    "ex:memberOf": "ex:engineering-team"
  }
}
```

### 2. Action (What)

Specifies which operation:

**Query:**
```json
{
  "f:action": "query"
}
```

**Transact:**
```json
{
  "f:action": "transact"
}
```

**Multiple Actions:**
```json
{
  "f:action": ["query", "transact"]
}
```

**All Actions:**
```json
{
  "f:action": "*"
}
```

### 3. Resource (Which Data)

Specifies what data the policy applies to:

**By Type:**
```json
{
  "f:resource": {
    "@type": "schema:Person"
  }
}
```

**By Predicate:**
```json
{
  "f:resource": {
    "f:predicate": "ex:salary"
  }
}
```

**Specific Entity:**
```json
{
  "f:resource": {
    "@id": "ex:alice"
  }
}
```

**Pattern with Variables:**
```json
{
  "f:resource": {
    "@type": "ex:Document",
    "ex:department": "?dept"
  }
}
```

**All Resources:**
```json
{
  "f:resource": "*"
}
```

### 4. Allow/Deny

Specifies whether to grant or deny access:

**Allow:**
```json
{
  "f:allow": true
}
```

**Deny:**
```json
{
  "f:allow": false
}
```

## Conditions

Policies can include conditions that must be satisfied:

```json
{
  "@id": "ex:same-department-policy",
  "f:subject": "?user",
  "f:action": "query",
  "f:resource": {
    "@type": "schema:Person",
    "ex:department": "?dept"
  },
  "f:condition": [
    { "@id": "?user", "ex:department": "?dept" }
  ],
  "f:allow": true
}
```

This allows users to query people in their own department.

### Multiple Conditions

```json
{
  "f:subject": "?user",
  "f:resource": {
    "@id": "?doc",
    "ex:status": "published"
  },
  "f:condition": [
    { "@id": "?user", "ex:clearanceLevel": "?level" },
    { "@id": "?doc", "ex:requiredClearance": "?reqLevel" },
    { "f:filter": "?level >= ?reqLevel" }
  ],
  "f:allow": true
}
```

## Policy Evaluation

### Input Context

When evaluating policies, Fluree has access to:

**Request Context:**
- **subject**: DID from signed request or authentication
- **action**: Operation being performed (query, transact)
- **resource**: Target entity/pattern being accessed
- **timestamp**: Current time

**Data Context:**
- **graph**: Current ledger state
- **entity properties**: Properties of entities being accessed
- **relationships**: Graph connections
- **history**: Historical data (if needed)

**Example:**
```text
Request: Query for ex:alice's data
Context:
  - subject: did:key:z6Mkh...
  - action: query
  - resource: ex:alice
  - graph: mydb:main@t:100
```

### Evaluation Steps

1. **Collect Applicable Policies**
   - Match subject (is this user covered?)
   - Match action (is this operation covered?)
   - Match resource (is this data covered?)

2. **Evaluate Conditions**
   - Execute condition queries
   - Check filters
   - Variable bindings must match

3. **Combine Results**
   - Apply combining algorithm
   - Resolve conflicts

4. **Return Decision**
   - Allow or Deny
   - With reasons (for debugging)

### Evaluation Example

**Policy:**
```json
{
  "f:subject": "did:key:z6Mkhabc...",
  "f:action": "query",
  "f:resource": { "@type": "ex:PublicData" },
  "f:allow": true
}
```

**Request:**
```text
subject: did:key:z6Mkhabc...
action: query
resource: ex:document-123 (type: ex:PublicData)
```

**Evaluation:**
```text
✓ Subject matches: did:key:z6Mkhabc...
✓ Action matches: query
✓ Resource matches: ex:document-123 is ex:PublicData
→ Result: ALLOW
```

## Combining Algorithms

### Deny Overrides (Default)

Most restrictive policy wins:

```text
Policy 1: ALLOW
Policy 2: DENY
→ Result: DENY
```

Logic:
1. If any policy denies → DENY
2. If any policy allows → ALLOW
3. If no policies match → DENY (default deny)

### Allow Overrides

Most permissive policy wins:

```text
Policy 1: DENY
Policy 2: ALLOW
→ Result: ALLOW
```

Logic:
1. If any policy allows → ALLOW
2. If any policy denies → DENY
3. If no policies match → DENY (default deny)

### First Applicable

First matching policy wins:

```text
Policy 1 (matches): ALLOW
Policy 2 (matches): DENY
→ Result: ALLOW (first match)
```

## Default Policies

### Default Deny

Recommended for production:

```json
{
  "@id": "ex:default-deny",
  "f:subject": "*",
  "f:action": "*",
  "f:resource": "*",
  "f:allow": false,
  "f:priority": -1000
}
```

All access denied unless explicitly allowed.

### Default Allow

For development only:

```json
{
  "@id": "ex:default-allow",
  "f:subject": "*",
  "f:action": "*",
  "f:resource": "*",
  "f:allow": true
}
```

All access allowed unless explicitly denied.

> **Note:** `default-allow` governs access for any requester — including unknown identities — once no matching policy restrictions apply. This is intentional for deployments where an application layer handles authorization and Fluree stores signed transactions for provenance. Set `default-allow: false` for fail-closed behavior when an identity is unknown or has no matching policy. See the [Policy Combining Algorithm](programmatic-policy.md#policy-combining-algorithm) for the three-state identity resolution.

## Policy Priority

Control policy evaluation order with priority:

```json
{
  "@id": "ex:admin-override",
  "f:subject": { "ex:role": "admin" },
  "f:action": "*",
  "f:allow": true,
  "f:priority": 1000
}
```

Higher priority policies evaluated first.

## Variable Binding

Variables in policies bind to values from context:

```json
{
  "f:subject": "?user",
  "f:resource": {
    "ex:owner": "?user"
  },
  "f:allow": true
}
```

**Evaluation:**
```text
Request subject: did:key:z6Mkhabc...
Bind: ?user = did:key:z6Mkhabc...

Check resource:
  ex:document-123 ex:owner did:key:z6Mkhabc...
  
Match! → ALLOW
```

## Pattern Matching

Policies can match patterns:

```json
{
  "f:resource": {
    "@type": "?type",
    "ex:visibility": "public"
  },
  "f:condition": [
    { "f:filter": "?type != ex:SensitiveData" }
  ],
  "f:allow": true
}
```

Allows access to any public data except SensitiveData.

## Time-Based Policies

Policies can be time-dependent:

```json
{
  "f:subject": "?user",
  "f:action": "query",
  "f:resource": {
    "ex:availableFrom": "?startDate",
    "ex:availableUntil": "?endDate"
  },
  "f:condition": [
    { "f:filter": "NOW() >= ?startDate && NOW() <= ?endDate" }
  ],
  "f:allow": true
}
```

## Property-Level Access Control

Control access to specific properties:

```json
{
  "@id": "ex:hide-salary",
  "f:subject": "*",
  "f:action": "query",
  "f:resource": {
    "f:predicate": "ex:salary"
  },
  "f:allow": false
}
```

```json
{
  "@id": "ex:show-salary-to-hr",
  "f:subject": { "ex:role": "hr" },
  "f:action": "query",
  "f:resource": {
    "f:predicate": "ex:salary"
  },
  "f:allow": true
}
```

## Entity-Level Access Control

Control access to specific entities:

```json
{
  "f:subject": "?user",
  "f:action": "*",
  "f:resource": {
    "@id": "?entity",
    "ex:owner": "?user"
  },
  "f:allow": true
}
```

Users can access entities they own.

## Policy Examples

### Public Read, Authenticated Write

```json
[
  {
    "@id": "ex:public-read",
    "f:subject": "*",
    "f:action": "query",
    "f:allow": true
  },
  {
    "@id": "ex:authenticated-write",
    "f:subject": "?user",
    "f:action": "transact",
    "f:condition": [
      { "@id": "?user", "@type": "ex:AuthenticatedUser" }
    ],
    "f:allow": true
  }
]
```

### Department Isolation

```json
{
  "@id": "ex:department-isolation",
  "f:subject": "?user",
  "f:action": "*",
  "f:resource": {
    "ex:department": "?dept"
  },
  "f:condition": [
    { "@id": "?user", "ex:department": "?dept" }
  ],
  "f:allow": true
}
```

### Hierarchical Permissions

```json
{
  "@id": "ex:manager-access",
  "f:subject": "?manager",
  "f:action": "*",
  "f:resource": {
    "ex:reportsTo": "?manager"
  },
  "f:allow": true
}
```

Managers can access data of their reports.

### Time-Window Access

```json
{
  "@id": "ex:business-hours-only",
  "f:subject": "?user",
  "f:action": "transact",
  "f:condition": [
    { "f:filter": "HOUR(NOW()) >= 9 && HOUR(NOW()) <= 17" }
  ],
  "f:allow": true
}
```

### Clearance-Level Access

```json
{
  "@id": "ex:clearance-policy",
  "f:subject": "?user",
  "f:resource": {
    "ex:classificationLevel": "?docLevel"
  },
  "f:condition": [
    { "@id": "?user", "ex:clearance": "?userLevel" },
    { "f:filter": "?userLevel >= ?docLevel" }
  ],
  "f:allow": true
}
```

## Policy Debugging

### Policy Trace

Enable policy tracing to see evaluation:

```bash
curl -X POST http://localhost:8090/v1/fluree/query \
  -H "X-Fluree-Policy-Trace: true" \
  -d '{...}'
```

Response includes trace:
```json
{
  "results": [...],
  "policy_trace": [
    {
      "policy": "ex:policy-1",
      "matched": true,
      "conditions_met": true,
      "decision": "allow"
    },
    {
      "policy": "ex:policy-2",
      "matched": false,
      "reason": "subject mismatch"
    }
  ],
  "final_decision": "allow"
}
```

### Test Policies

Test policy evaluation:

```javascript
async function testPolicy(policyId, testCases) {
  for (const test of testCases) {
    const result = await evaluatePolicy({
      policy: policyId,
      subject: test.subject,
      action: test.action,
      resource: test.resource
    });
    
    console.log(`Test: ${test.name}`);
    console.log(`Expected: ${test.expected}`);
    console.log(`Actual: ${result.decision}`);
    console.log(`Match: ${result.decision === test.expected ? 'PASS' : 'FAIL'}`);
  }
}
```

## Best Practices

### 1. Start with Default Deny

```json
{
  "f:subject": "*",
  "f:action": "*",
  "f:allow": false,
  "f:priority": -1000
}
```

### 2. Use Specific Policies

Prefer specific over general:

Good:
```json
{
  "f:resource": { "@type": "ex:PublicDocument" },
  "f:allow": true
}
```

Less secure:
```json
{
  "f:resource": "*",
  "f:allow": true
}
```

### 3. Organize by Role

Group policies by role:

```json
{
  "@id": "ex:admin-policies",
  "@type": "ex:PolicySet",
  "ex:includes": [
    "ex:admin-query-policy",
    "ex:admin-transact-policy",
    "ex:admin-delete-policy"
  ]
}
```

### 4. Document Policies

Add descriptions:

```json
{
  "@id": "ex:policy-1",
  "rdfs:label": "Public read access",
  "rdfs:comment": "Allows anyone to read public documents",
  "f:subject": "*",
  "f:action": "query",
  "f:resource": { "ex:visibility": "public" },
  "f:allow": true
}
```

### 5. Test Thoroughly

Test all policy paths:
- Positive cases (should allow)
- Negative cases (should deny)
- Edge cases
- Condition evaluation

### 6. Monitor Policy Usage

Log policy decisions:

```javascript
policyLogger.info({
  timestamp: new Date(),
  subject: request.subject,
  action: request.action,
  resource: request.resource,
  decision: policyResult.decision,
  policies_evaluated: policyResult.policies
});
```

## Related Documentation

- [Policy in Queries](policy-in-queries.md) - Query-time enforcement
- [Policy in Transactions](policy-in-transactions.md) - Transaction-time enforcement
- [Signed Requests](../api/signed-requests.md) - Authentication
- [Policy Enforcement Concepts](../concepts/policy-enforcement.md) - High-level overview
