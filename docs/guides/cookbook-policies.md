# Cookbook: Access Control Policies

Fluree policies enforce access control at the database level — individual facts (flakes) are filtered based on who's asking. The same query returns different results for different users, automatically. No application-layer filtering needed.

## Quick start

### 1. Set up sample data

```bash
fluree insert '{
  "@context": {
    "schema": "http://schema.org/",
    "ex": "http://example.org/"
  },
  "@graph": [
    {
      "@id": "ex:alice",
      "@type": "schema:Person",
      "schema:name": "Alice Chen",
      "ex:role": "engineer",
      "ex:department": "platform",
      "ex:salary": 130000
    },
    {
      "@id": "ex:bob",
      "@type": "schema:Person",
      "schema:name": "Bob Martinez",
      "ex:role": "manager",
      "ex:department": "platform",
      "ex:salary": 155000
    },
    {
      "@id": "ex:carol",
      "@type": "schema:Person",
      "schema:name": "Carol White",
      "ex:role": "engineer",
      "ex:department": "marketing",
      "ex:salary": 115000
    }
  ]
}'
```

### 2. Add policies

Policies are data in the ledger — insert them like any other data:

```bash
fluree insert '{
  "@context": {
    "f": "https://ns.flur.ee/db#",
    "ex": "http://example.org/",
    "schema": "http://schema.org/"
  },
  "@graph": [
    {
      "@id": "ex:policy-see-own-dept",
      "@type": "f:Policy",
      "f:subject": "?user",
      "f:action": "query",
      "f:resource": {
        "@type": "schema:Person",
        "ex:department": "?dept"
      },
      "f:condition": [
        {"@id": "?user", "ex:department": "?dept"}
      ],
      "f:allow": true
    },
    {
      "@id": "ex:policy-hide-salary",
      "@type": "f:Policy",
      "f:action": "query",
      "f:resource": {"f:predicate": "ex:salary"},
      "f:allow": false
    },
    {
      "@id": "ex:policy-manager-sees-salary",
      "@type": "f:Policy",
      "f:subject": "?user",
      "f:action": "query",
      "f:resource": {
        "f:predicate": "ex:salary",
        "ex:department": "?dept"
      },
      "f:condition": [
        {"@id": "?user", "ex:role": "manager", "ex:department": "?dept"}
      ],
      "f:allow": true
    }
  ]
}'
```

These three policies create:
- **Department isolation** — Users can only see people in their own department
- **Salary hidden by default** — Nobody sees salary data
- **Managers see department salaries** — Managers can see salaries for their own department

### 3. Query as different users

**As Alice** (engineer, platform):

```bash
fluree query '{
  "@context": {"schema": "http://schema.org/", "ex": "http://example.org/"},
  "select": ["?name", "?salary"],
  "where": [
    {"@id": "?person", "schema:name": "?name"},
    ["optional", {"@id": "?person", "ex:salary": "?salary"}]
  ],
  "opts": {"identity": "ex:alice"}
}'
```

Alice sees names of platform team members (Alice, Bob) but no salaries.

**As Bob** (manager, platform):

```bash
fluree query '{
  "@context": {"schema": "http://schema.org/", "ex": "http://example.org/"},
  "select": ["?name", "?salary"],
  "where": [
    {"@id": "?person", "schema:name": "?name"},
    ["optional", {"@id": "?person", "ex:salary": "?salary"}]
  ],
  "opts": {"identity": "ex:bob"}
}'
```

Bob sees names and salaries for platform team members, but not Carol (marketing).

## Patterns

### Public read, authenticated write

```json
[
  {
    "@id": "ex:public-read",
    "@type": "f:Policy",
    "f:subject": "*",
    "f:action": "query",
    "f:allow": true
  },
  {
    "@id": "ex:authenticated-write",
    "@type": "f:Policy",
    "f:subject": "?user",
    "f:action": "transact",
    "f:condition": [
      {"@id": "?user", "@type": "ex:AuthenticatedUser"}
    ],
    "f:allow": true
  }
]
```

### Owner-only access

Users can only access entities they own:

```json
{
  "@id": "ex:owner-only",
  "@type": "f:Policy",
  "f:subject": "?user",
  "f:action": "*",
  "f:resource": {"ex:owner": "?user"},
  "f:allow": true
}
```

### Visibility levels

Public, internal, and confidential content:

```json
[
  {
    "@id": "ex:public-visible",
    "@type": "f:Policy",
    "f:subject": "*",
    "f:action": "query",
    "f:resource": {"ex:visibility": "public"},
    "f:allow": true
  },
  {
    "@id": "ex:internal-visible",
    "@type": "f:Policy",
    "f:subject": "?user",
    "f:action": "query",
    "f:resource": {"ex:visibility": "internal"},
    "f:condition": [
      {"@id": "?user", "@type": "ex:Employee"}
    ],
    "f:allow": true
  },
  {
    "@id": "ex:confidential-visible",
    "@type": "f:Policy",
    "f:subject": "?user",
    "f:action": "query",
    "f:resource": {"ex:visibility": "confidential"},
    "f:condition": [
      {"@id": "?user", "ex:role": "manager"}
    ],
    "f:allow": true
  }
]
```

### Property redaction

Hide specific properties from unauthorized users:

```json
[
  {
    "@id": "ex:hide-ssn",
    "@type": "f:Policy",
    "f:action": "query",
    "f:resource": {"f:predicate": "ex:ssn"},
    "f:allow": false
  },
  {
    "@id": "ex:hr-sees-ssn",
    "@type": "f:Policy",
    "f:subject": "?user",
    "f:action": "query",
    "f:resource": {"f:predicate": "ex:ssn"},
    "f:condition": [
      {"@id": "?user", "ex:role": "hr"}
    ],
    "f:allow": true
  }
]
```

### Hierarchical access (manager sees reports)

```json
{
  "@id": "ex:manager-sees-reports",
  "@type": "f:Policy",
  "f:subject": "?manager",
  "f:action": "query",
  "f:resource": {"ex:reportsTo": "?manager"},
  "f:allow": true
}
```

### Multi-tenant isolation

Each tenant sees only their data:

```json
{
  "@id": "ex:tenant-isolation",
  "@type": "f:Policy",
  "f:subject": "?user",
  "f:action": "*",
  "f:resource": {"ex:tenant": "?tenantId"},
  "f:condition": [
    {"@id": "?user", "ex:tenant": "?tenantId"}
  ],
  "f:allow": true
}
```

### Default deny

For production, start with a default-deny policy:

```json
{
  "@id": "ex:default-deny",
  "@type": "f:Policy",
  "f:subject": "*",
  "f:action": "*",
  "f:resource": "*",
  "f:allow": false,
  "f:priority": -1000
}
```

Then add specific allow policies. Higher priority policies are evaluated first.

## Policy evaluation rules

1. **Deny overrides** (default) — If any policy denies access, it's denied
2. **No matching policy = deny** — Unlisted resources are inaccessible
3. **Conditions must match** — If a condition query returns no results, the policy doesn't apply
4. **Variables bind from context** — `?user` binds to the requesting identity's IRI

## HTTP API with policies

Policies are enforced via the `Authorization` header:

```bash
# Query with a specific identity
curl -X POST 'http://localhost:8090/v1/fluree/query?ledger=mydb:main' \
  -H "Authorization: Bearer <JWT token>" \
  -H "Content-Type: application/sparql-query" \
  -d 'SELECT ?name ?salary WHERE { ?p schema:name ?name . OPTIONAL { ?p ex:salary ?salary } }'
```

The JWT token's subject claim is used as the identity for policy evaluation.

## Policies are data

Because policies are stored as flakes in the ledger:

- **Time-travelable** — See what policies were in effect at any point in history
- **Auditable** — Query the policies themselves: `SELECT ?policy ?action WHERE { ?policy a f:Policy ; f:action ?action }`
- **Versionable** — Policies change through normal transactions, with full history
- **Branchable** — Test new policies on a branch before merging to main

## Best practices

1. **Start with default deny** — Explicitly allow what's needed
2. **Use type-based policies** — They're the most efficient (use OPST index)
3. **Test with multiple identities** — Verify the same query returns correct results for each role
4. **Document policy intent** — Add `rdfs:label` and `rdfs:comment` to policy entities
5. **Separate read and write** — Different policies for `query` vs `transact`
6. **Monitor performance** — Complex condition queries add overhead

## Related documentation

- [Policy Model](../security/policy-model.md) — Full policy structure reference
- [Policy in Queries](../security/policy-in-queries.md) — Query-time enforcement details
- [Policy in Transactions](../security/policy-in-transactions.md) — Write-time enforcement
- [Authentication](../security/authentication.md) — Identity and token setup
- [Policy Concepts](../concepts/policy-enforcement.md) — Architecture overview
