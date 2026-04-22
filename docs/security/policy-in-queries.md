# Policy in Queries

Policies are enforced at query time, filtering results to show only data the user is authorized to see. This document explains how policies affect query execution.

## Query-Time Filtering

When a query executes, Fluree:
1. Parses the query
2. Identifies the subject (from signed request or auth)
3. Collects applicable policies
4. Augments query with policy filters
5. Executes augmented query
6. Returns filtered results

**User never sees unauthorized data.**

## Basic Example

### Without Policy

Query:
```sparql
SELECT ?name
WHERE {
  ?person schema:name ?name .
}
```

Result (all people):
```json
[
  { "name": "Alice" },
  { "name": "Bob" },
  { "name": "Carol" },
  { "name": "David" }
]
```

### With Policy

Policy:
```json
{
  "f:subject": "did:key:z6Mkhabc...",
  "f:action": "query",
  "f:resource": {
    "ex:department": "engineering"
  },
  "f:allow": true
}
```

Same query, filtered result (only engineering):
```json
[
  { "name": "Alice" },
  { "name": "Bob" }
]
```

Carol and David are in other departments, so filtered out.

## Policy Application

### Type-Based Filtering

Policy limiting to specific type:

```json
{
  "f:subject": "*",
  "f:action": "query",
  "f:resource": {
    "@type": "ex:PublicDocument"
  },
  "f:allow": true
}
```

Query:
```sparql
SELECT ?title
WHERE {
  ?doc schema:title ?title .
}
```

Augmented query (automatically):
```sparql
SELECT ?title
WHERE {
  ?doc a ex:PublicDocument .
  ?doc schema:title ?title .
}
```

Only public documents returned.

### Property-Based Filtering

Policy on specific property:

```json
{
  "f:subject": { "ex:role": "hr" },
  "f:action": "query",
  "f:resource": {
    "f:predicate": "ex:salary"
  },
  "f:allow": true
}
```

For non-HR users, queries requesting salary:
```sparql
SELECT ?name ?salary
WHERE {
  ?person schema:name ?name .
  ?person ex:salary ?salary .
}
```

Result (salary filtered out):
```json
[
  { "name": "Alice", "salary": null },
  { "name": "Bob", "salary": null }
]
```

For HR users, full data returned.

### Entity-Level Filtering

Policy for entity ownership:

```json
{
  "f:subject": "?user",
  "f:action": "query",
  "f:resource": {
    "ex:owner": "?user"
  },
  "f:allow": true
}
```

Query:
```sparql
SELECT ?title
WHERE {
  ?doc schema:title ?title .
}
```

Augmented:
```sparql
SELECT ?title
WHERE {
  ?doc ex:owner <did:key:z6Mkhabc...> .
  ?doc schema:title ?title .
}
```

Only documents owned by user returned.

## Pattern Matching

Policies can match complex patterns:

```json
{
  "f:subject": "?user",
  "f:resource": {
    "@type": "ex:Document",
    "ex:department": "?dept"
  },
  "f:condition": [
    { "@id": "?user", "ex:department": "?dept" }
  ],
  "f:allow": true
}
```

This allows users to see documents from their department.

Query:
```json
{
  "select": ["?title"],
  "where": [
    { "@id": "?doc", "schema:title": "?title" }
  ]
}
```

Augmented with policy:
```json
{
  "select": ["?title"],
  "where": [
    { "@id": "?doc", "schema:title": "?title" },
    { "@id": "?doc", "@type": "ex:Document" },
    { "@id": "?doc", "ex:department": "?dept" },
    { "@id": "?user", "ex:department": "?dept" }
  ],
  "bind": {
    "?user": "did:key:z6Mkhabc..."
  }
}
```

## Multi-Policy Scenarios

### Combining Policies

Multiple policies can apply:

**Policy 1 (allow public):**
```json
{
  "f:resource": { "ex:visibility": "public" },
  "f:allow": true
}
```

**Policy 2 (allow owned):**
```json
{
  "f:subject": "?user",
  "f:resource": { "ex:owner": "?user" },
  "f:allow": true
}
```

Query returns:
- Public documents (from policy 1)
- User's private documents (from policy 2)

### Deny Overrides

Deny policies override allow:

**Policy 1 (allow department):**
```json
{
  "f:resource": { "ex:department": "engineering" },
  "f:allow": true
}
```

**Policy 2 (deny sensitive):**
```json
{
  "f:resource": { "ex:classification": "confidential" },
  "f:allow": false
}
```

Query returns:
- Engineering documents (from policy 1)
- EXCEPT confidential ones (blocked by policy 2)

## SPARQL Queries

SPARQL queries are filtered the same way:

```sparql
PREFIX ex: <http://example.org/ns/>
PREFIX schema: <http://schema.org/>

SELECT ?name ?email
WHERE {
  ?person a schema:Person .
  ?person schema:name ?name .
  ?person schema:email ?email .
}
```

With policy limiting to public profiles:
```sparql
PREFIX ex: <http://example.org/ns/>
PREFIX schema: <http://schema.org/>

SELECT ?name ?email
WHERE {
  ?person a schema:Person .
  ?person ex:profileVisibility "public" .  # Added by policy
  ?person schema:name ?name .
  ?person schema:email ?email .
}
```

## JSON-LD Queries

JSON-LD queries work similarly:

Original query:
```json
{
  "select": ["?name", "?age"],
  "where": [
    { "@id": "?person", "schema:name": "?name" },
    { "@id": "?person", "schema:age": "?age" }
  ]
}
```

With policy (department-based):
```json
{
  "select": ["?name", "?age"],
  "where": [
    { "@id": "?person", "schema:name": "?name" },
    { "@id": "?person", "schema:age": "?age" },
    { "@id": "?person", "ex:department": "?dept" },
    { "@id": "<did:key:z6Mkh...>", "ex:department": "?dept" }
  ]
}
```

## Performance Considerations

### Efficient Policies

Type-based policies are efficient:

Good:
```json
{
  "f:resource": { "@type": "ex:PublicData" }
}
```

Less efficient:
```json
{
  "f:resource": { "?pred": "?val" },
  "f:condition": [
    { "f:filter": "complex condition" }
  ]
}
```

### Index Usage

Policy filters use indexes:
- Type filters: Use OPST index
- Property filters: Use POST/PSOT indexes
- Entity filters: Use SPOT index

### Query Planning

Fluree optimizes policy-augmented queries:
- Pushes filters down
- Reorders joins
- Selects optimal indexes

Check query plan:
```bash
curl -X POST http://localhost:8090/v1/fluree/explain \
  -d '{...}'
```

## Property Redaction

Hide specific properties:

Policy:
```json
{
  "f:subject": "*",
  "f:action": "query",
  "f:resource": {
    "f:predicate": "ex:ssn"
  },
  "f:allow": false
}
```

Query:
```json
{
  "select": ["?name", "?ssn"],
  "where": [
    { "@id": "?person", "schema:name": "?name" },
    { "@id": "?person", "ex:ssn": "?ssn" }
  ]
}
```

Result (SSN redacted):
```json
[
  { "name": "Alice", "ssn": null },
  { "name": "Bob", "ssn": null }
]
```

## Aggregate Queries

Policies apply to aggregates:

Query:
```sparql
SELECT (COUNT(?person) AS ?count)
WHERE {
  ?person a schema:Person .
}
```

With policy (department-based), count includes only authorized people.

## Graph Crawl Queries

Policies apply to graph crawls:

```json
{
  "select": "?person",
  "where": [
    { "@id": "ex:alice", "schema:knows": "?person" }
  ],
  "depth": 3
}
```

Crawl follows links only to authorized entities.

## Multi-Graph Queries

Policies per graph:

```sparql
SELECT ?data
FROM <public:main>
FROM <private:main>
WHERE {
  ?entity ex:data ?data .
}
```

Policy for public graph allows all, policy for private graph restricts.

## Time Travel Queries

Policies apply to historical queries:

```json
{
  "from": "mydb:main@t:100",
  "select": ["?name"],
  "where": [
    { "@id": "?person", "schema:name": "?name" }
  ]
}
```

Same policies apply, showing historical data user was authorized to see.

## Debugging Policy Filtering

### Enable Policy Trace

```bash
curl -X POST http://localhost:8090/v1/fluree/query \
  -H "X-Fluree-Policy-Trace: true" \
  -d '{...}'
```

Response:
```json
{
  "results": [...],
  "policy_trace": {
    "policies_applied": [
      {
        "id": "ex:policy-1",
        "effect": "filter_added",
        "filter": "?person ex:department 'engineering'"
      }
    ],
    "original_pattern_count": 2,
    "augmented_pattern_count": 4,
    "execution_time_ms": 45
  }
}
```

### Check Effective Query

See augmented query:

```bash
curl -X POST http://localhost:8090/v1/fluree/query \
  -H "X-Fluree-Show-Augmented-Query: true" \
  -d '{...}'
```

Response includes augmented query showing policy filters.

## Common Patterns

### Public Read, Private Write

```json
[
  {
    "@id": "ex:public-read",
    "f:subject": "*",
    "f:action": "query",
    "f:resource": { "ex:visibility": "public" },
    "f:allow": true
  },
  {
    "@id": "ex:owner-full-access",
    "f:subject": "?user",
    "f:action": "*",
    "f:resource": { "ex:owner": "?user" },
    "f:allow": true
  }
]
```

### Department Isolation

```json
{
  "f:subject": "?user",
  "f:resource": { "ex:department": "?dept" },
  "f:condition": [
    { "@id": "?user", "ex:department": "?dept" }
  ],
  "f:allow": true
}
```

### Role-Based Views

```json
[
  {
    "@id": "ex:manager-view",
    "f:subject": { "ex:role": "manager" },
    "f:action": "query",
    "f:resource": { "ex:visibility": ["public", "internal"] },
    "f:allow": true
  },
  {
    "@id": "ex:employee-view",
    "f:subject": { "ex:role": "employee" },
    "f:action": "query",
    "f:resource": { "ex:visibility": "public" },
    "f:allow": true
  }
]
```

## Best Practices

### 1. Test Policy Effects

Test queries with different subjects:

```javascript
async function testQueryWithPolicies() {
  const query = { select: ["?name"], where: [...] };
  
  const adminResult = await queryAs("admin", query);
  const userResult = await queryAs("user", query);
  
  console.log(`Admin sees ${adminResult.length} results`);
  console.log(`User sees ${userResult.length} results`);
}
```

### 2. Monitor Query Performance

Track policy overhead:

```javascript
const start = Date.now();
const result = await query({...});
const duration = Date.now() - start;

if (duration > 1000) {
  logger.warn(`Slow query with policies: ${duration}ms`);
}
```

### 3. Use Specific Filters

Make policy filters as specific as possible for better performance.

### 4. Document Policy Intent

```json
{
  "@id": "ex:policy-1",
  "rdfs:label": "Engineering department access",
  "rdfs:comment": "Allows engineering team to view all technical documents",
  ...
}
```

### 5. Test Edge Cases

- Empty result sets
- Large result sets
- Complex graph patterns
- Multi-graph queries

## Testing Policies from the CLI

The `fluree` CLI supports policy-enforced queries so you can verify that the
policies you've configured filter results as expected — without writing any
client code.

### Flags

Available on `fluree query` (and on `fluree insert`, `upsert`, `update` for
write-time enforcement):

| Flag | Purpose |
|------|---------|
| `--as <IRI>` | Execute as this identity. Resolves `f:policyClass` on the identity subject to collect applicable policies. |
| `--policy-class <IRI>` | Apply policies of the given class IRI. Repeatable. Narrows to the intersection with the identity's policies, or applies directly without `--as`. |
| `--default-allow` | Allow when no matching policy exists for the operation. Defaults to false (deny-by-default). |

### Workflow

1. Transact your policy rules (and the identities with their `f:policyClass`
   assignments) into the ledger, using any of the normal insert / upsert /
   update commands.
2. Re-run the same query as different identities to confirm results differ as
   the policies prescribe:

```bash
# Full result set (no policy enforcement)
fluree query 'SELECT ?name ?salary WHERE { ?p schema:name ?name ; ex:salary ?salary }'

# As an HR user — should see all salaries
fluree query --as did:key:z6MkHR... \
  'SELECT ?name ?salary WHERE { ?p schema:name ?name ; ex:salary ?salary }'

# As a regular employee — policies should hide salary field
fluree query --as did:key:z6MkEmp... \
  'SELECT ?name ?salary WHERE { ?p schema:name ?name ; ex:salary ?salary }'
```

Combine with `--policy-class` to scope to a specific policy set, or with
`--default-allow` to flip the baseline for negative testing:

```bash
fluree query --as did:key:z6Mkh... --policy-class ex:ReadOnlyEngineer \
  'SELECT ?doc WHERE { ?doc a ex:Document }'
```

### Local vs Remote

The flags work in both modes:

- **Local** (default, or with `--direct`): the CLI loads the ledger directly
  and applies policy via the in-process query engine.
- **Remote** (with `--remote <name>`, or auto-routed through a running local
  server): the CLI sends the flags to the server as HTTP headers
  (`fluree-identity`, `fluree-policy-class`, `fluree-default-allow`) and, for
  JSON-LD bodies, also injects them into `opts`. Multi-value `--policy-class`
  rides through the body opts only; SPARQL transport is single-valued via the
  header.

### Remote impersonation: how it's authorized

When you run against a remote server with `--as <iri>`, the server treats the
request as **impersonation** and gates it as follows:

1. Your bearer token's identity is resolved on the target ledger.
2. If that identity has **no** `f:policyClass` assignments
   (the `FoundNoPolicies` outcome — i.e., your service account is unrestricted
   on this ledger), the server honors `--as` and runs the query as the target
   identity.
3. If your bearer identity is itself policy-constrained
   (`FoundWithPolicies`) or unknown to this ledger (`NotFound`), the server
   force-overrides `--as` with your bearer identity. You see your own filtered
   view, not the target's.

Each successful impersonation is logged at `info` level on the server:

```
policy impersonation: bearer=<svc-id> target=<as-iri> ledger=<name>
```

This is the standard service-account pattern: register your CLI/app-server
identity in the ledger with no `f:policyClass`, and it gains the right to
delegate to any end-user identity for testing or per-request enforcement.
Assigning a policy class to that identity revokes the delegation right with no
config change.

### Limitations

- Inline policy rules (`opts.policy`) and policy variable bindings
  (`opts.policy-values`) are not yet exposed as CLI flags — use a JSON-LD
  query body with an `"opts"` block when you need those.
- For SPARQL queries against a remote, only `--as` and `--policy-class` (single
  value) and `--default-allow` are wired (via headers). Multi-value
  `--policy-class` works on JSON-LD only.
- Proxy-mode servers fall back to the legacy non-impersonation behavior — the
  upstream server performs the impersonation check.

## Related Documentation

- [Policy Model](policy-model.md) - Policy structure and evaluation
- [Policy in Transactions](policy-in-transactions.md) - Write-time enforcement
- [Query Documentation](../query/README.md) - Query syntax
- [Explain Plans](../query/explain.md) - Query optimization
