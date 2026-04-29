# Cookbook: Access Control Policies

Fluree policies enforce access control inside the database — individual facts (flakes) are filtered based on the requesting identity. The same query returns different results for different users, automatically. No application-layer filtering needed.

This cookbook walks through the common patterns. For the underlying model see [Policy enforcement](../concepts/policy-enforcement.md); for the full reference see [Policy model and inputs](../security/policy-model.md).

## How a policy is shaped

Every policy is a JSON-LD node typed `f:AccessPolicy`. It has three orthogonal pieces:

| Field | Purpose |
|-------|---------|
| **What it targets** | `f:onProperty`, `f:onClass`, `f:onSubject` (any combination, each an array of `@id` references). Omit all three to make a default policy that applies to every flake. |
| **What it governs** | `f:action` — `f:view` (queries), `f:modify` (transactions), or both. |
| **Whether it permits** | Either `f:allow: true` (unconditional allow), `f:allow: false` (deny), or `f:query: "<JSON-encoded WHERE>"` (allow when the embedded query returns at least one binding for the target). |

Two more knobs:

- `f:required: true` — the policy *must* allow for access to be granted on its targets, even if `default-allow` is true. Use it for hard constraints.
- `f:exMessage` — error message returned to the caller when the policy denies a transaction.

Inside `f:query`, two special variables are pre-bound: `?$this` (the entity being checked) and `?$identity` (the requesting identity, supplied via `policy-values`).

## Quick start

### 1. Insert sample data

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

Add identity records that link DIDs / users to the entities they represent:

```bash
fluree insert '{
  "@context": {"ex": "http://example.org/", "f": "https://ns.flur.ee/db#"},
  "@graph": [
    { "@id": "ex:aliceIdentity", "ex:user": {"@id": "ex:alice"},
      "f:policyClass": [{"@id": "ex:CorpPolicy"}] },
    { "@id": "ex:bobIdentity",   "ex:user": {"@id": "ex:bob"},
      "f:policyClass": [{"@id": "ex:CorpPolicy"}] }
  ]
}'
```

`f:policyClass` tags an identity with the set of policy classes that apply to it — every stored policy of that class will be loaded automatically when this identity makes a request.

### 2. Insert policies

Policies are data — they go into the ledger like any other graph:

```bash
fluree insert '{
  "@context": {
    "f": "https://ns.flur.ee/db#",
    "ex": "http://example.org/",
    "schema": "http://schema.org/"
  },
  "@graph": [
    {
      "@id": "ex:salary-restriction",
      "@type": ["f:AccessPolicy", "ex:CorpPolicy"],
      "f:required": true,
      "f:onProperty": [{"@id": "ex:salary"}],
      "f:action": [{"@id": "f:view"}],
      "f:query": "{\"where\": {\"@id\": \"?$identity\", \"http://example.org/user\": {\"@id\": \"?$subject\"}, \"http://example.org/role\": \"manager\", \"http://example.org/department\": \"?dept\"}, \"$where\": {\"@id\": \"?$this\", \"http://example.org/department\": \"?dept\"}}"
    },
    {
      "@id": "ex:default-view",
      "@type": ["f:AccessPolicy", "ex:CorpPolicy"],
      "f:action": [{"@id": "f:view"}],
      "f:allow": true
    }
  ]
}'
```

What this set of two policies says:

1. **`ex:salary-restriction`** is **required** for `ex:salary`: a request can read `ex:salary` only when `f:query` returns a binding. The query says: *given the identity, find the user it represents; if that user is a manager in the same department as the entity being viewed (`?$this`), allow*.
2. **`ex:default-view`** allows reading everything else.

`f:query` is stored as a JSON string inside the policy because RDF can't hold structured JSON natively. When loaded, the engine parses it and runs it as a subquery with `?$this` and `?$identity` pre-bound.

### 3. Query as different identities

**As Alice (engineer in platform — no manager privilege)**:

```bash
fluree query '{
  "@context": {"schema": "http://schema.org/", "ex": "http://example.org/"},
  "from": "mydb:main",
  "select": ["?name", "?salary"],
  "where": [
    {"@id": "?p", "schema:name": "?name"},
    ["optional", {"@id": "?p", "ex:salary": "?salary"}]
  ],
  "opts": {
    "identity": "ex:aliceIdentity",
    "policy-class": ["ex:CorpPolicy"],
    "default-allow": false
  }
}'
```

Alice sees every name but no salaries — the required policy denies `ex:salary` because she isn't a manager.

**As Bob (manager in platform)**:

Same query, but `"identity": "ex:bobIdentity"`. Bob sees salaries for Alice and Bob (same department) but Carol's salary stays hidden — different department.

## Inline policies (no insert needed)

Don't want to commit policies to the ledger yet? Pass them inline via `opts.policy`:

```json
{
  "from": "mydb:main",
  "select": "?name",
  "where": [{"@id": "?p", "schema:name": "?name"}],
  "opts": {
    "policy": [
      {
        "@id": "ex:adhoc-allow",
        "@type": "f:AccessPolicy",
        "f:action": "f:view",
        "f:allow": true
      }
    ],
    "default-allow": false
  }
}
```

Inline policies are useful for one-off queries, automated tests, and admin scripts. Stored policies (with `policy-class`) are the right approach for production access control because they're versioned, time-travelable, and consistent across all requests.

## Patterns

### Public read

```json
{
  "@id": "ex:public-read",
  "@type": "f:AccessPolicy",
  "f:action": [{"@id": "f:view"}],
  "f:allow": true
}
```

A default-allow policy with no targeting applies to every flake.

### Owner-only access

```json
{
  "@id": "ex:owner-only",
  "@type": ["f:AccessPolicy", "ex:CorpPolicy"],
  "f:required": true,
  "f:action": [{"@id": "f:view"}, {"@id": "f:modify"}],
  "f:query": "{\"where\": {\"@id\": \"?$identity\", \"http://example.org/user\": {\"@id\": \"?$user\"}}, \"$where\": {\"@id\": \"?$this\", \"http://example.org/owner\": {\"@id\": \"?$user\"}}}"
}
```

The query resolves `?$identity → user`, then checks that `?$this` (the entity being read or written) has that user as its `ex:owner`.

### Property redaction (hide a property unless permitted)

```json
[
  {
    "@id": "ex:hide-ssn",
    "@type": ["f:AccessPolicy", "ex:CorpPolicy"],
    "f:required": true,
    "f:onProperty": [{"@id": "ex:ssn"}],
    "f:action": [{"@id": "f:view"}],
    "f:query": "{\"where\": {\"@id\": \"?$identity\", \"http://example.org/role\": \"hr\"}}"
  },
  {
    "@id": "ex:default-view",
    "@type": ["f:AccessPolicy", "ex:CorpPolicy"],
    "f:action": [{"@id": "f:view"}],
    "f:allow": true
  }
]
```

`f:onProperty` scopes the restriction to `ex:ssn` only — every other property still falls under `ex:default-view`. `f:required: true` means the SSN policy MUST allow for any SSN flake to be visible (the default allow doesn't override it on this property).

### Class-scoped restriction

```json
{
  "@id": "ex:employee-only",
  "@type": ["f:AccessPolicy", "ex:CorpPolicy"],
  "f:required": true,
  "f:onClass": [{"@id": "ex:Employee"}],
  "f:action": [{"@id": "f:view"}],
  "f:query": "{\"where\": {\"@id\": \"?$identity\", \"@type\": \"http://example.org/Employee\"}}"
}
```

Anyone querying for `ex:Employee` instances must themselves be tagged as an employee.

### Multi-tenant isolation

```json
{
  "@id": "ex:tenant-isolation",
  "@type": ["f:AccessPolicy", "ex:CorpPolicy"],
  "f:required": true,
  "f:action": [{"@id": "f:view"}, {"@id": "f:modify"}],
  "f:query": "{\"where\": {\"@id\": \"?$identity\", \"http://example.org/tenant\": \"?tenant\"}, \"$where\": {\"@id\": \"?$this\", \"http://example.org/tenant\": \"?tenant\"}}"
}
```

Each tenant only sees and writes data tagged with their own `ex:tenant`. Required-no-targeting means it applies to every flake.

### Hierarchical access (manager sees direct reports)

```json
{
  "@id": "ex:manager-sees-reports",
  "@type": ["f:AccessPolicy", "ex:CorpPolicy"],
  "f:onClass": [{"@id": "schema:Person"}],
  "f:action": [{"@id": "f:view"}],
  "f:query": "{\"where\": {\"@id\": \"?$identity\", \"http://example.org/user\": {\"@id\": \"?$mgr\"}}, \"$where\": {\"@id\": \"?$this\", \"http://example.org/reportsTo\": {\"@id\": \"?$mgr\"}}}"
}
```

### Write protection

```json
{
  "@id": "ex:no-direct-writes",
  "@type": ["f:AccessPolicy", "ex:CorpPolicy"],
  "f:required": true,
  "f:onProperty": [{"@id": "ex:approved"}],
  "f:action": [{"@id": "f:modify"}],
  "f:exMessage": "ex:approved is set by the workflow service only.",
  "f:query": "{\"where\": {\"@id\": \"?$identity\", \"@type\": \"http://example.org/WorkflowService\"}}"
}
```

When the policy denies a transaction, `f:exMessage` is returned to the client.

## Combining algorithm

When multiple policies match a flake:

- A **required** policy must allow. If any required policy denies (or returns no `f:query` bindings), access is denied.
- If no required policy applies, **any** allow is enough — Fluree uses *allow-overrides* over the non-required set.
- If no policy applies, the request falls back to `default-allow`. Setting `default-allow: false` is the fail-closed default for production.

See [Policy model and inputs](../security/policy-model.md#policy-combining-algorithm) for the full state diagram.

## Invoking policies via HTTP

Policies are passed via opts on JSON-LD requests, and via headers on SPARQL requests.

### JSON-LD

```bash
curl -X POST 'http://localhost:8090/v1/fluree/query?ledger=mydb:main' \
  -H 'Content-Type: application/json' \
  -H "Authorization: Bearer $JWT" \
  -d '{
    "from": "mydb:main",
    "select": "?name",
    "where": [{"@id": "?p", "schema:name": "?name"}],
    "opts": {
      "identity": "ex:aliceIdentity",
      "policy-class": ["ex:CorpPolicy"],
      "default-allow": false
    }
  }'
```

### SPARQL (headers — no `opts` block in SPARQL)

```bash
curl -X POST 'http://localhost:8090/v1/fluree/query?ledger=mydb:main' \
  -H 'Content-Type: application/sparql-query' \
  -H "Authorization: Bearer $JWT" \
  -H 'fluree-identity: ex:aliceIdentity' \
  -H 'fluree-policy-class: ex:CorpPolicy' \
  -H 'fluree-default-allow: false' \
  -d 'SELECT ?name WHERE { ?p <http://schema.org/name> ?name }'
```

| Header | JSON-LD `opts` field | Value |
|--------|----------------------|-------|
| `fluree-identity` | `identity` | IRI of an identity entity |
| `fluree-policy-class` | `policy-class` | Comma-separated or repeated header; matches `f:policyClass` on stored policies |
| `fluree-policy-values` | `policy-values` | JSON object — extra `?$var` bindings for policy queries |
| `fluree-policy` | `policy` | Inline JSON-LD policy array |
| `fluree-default-allow` | `default-allow` | `true` / `false` |

When the bearer token is verified and the server is configured with `data_auth_default_policy_class`, the verified identity is auto-applied to `policy-values` and the configured class to `policy-class`. See [Configuration](../operations/configuration.md) for those server-side settings.

## Policies are data

Because policies live as flakes in the ledger:

- **Time-travel** — query at any past `t` to see the policies in effect then.
- **Audit** — `SELECT ?p ?action WHERE { ?p a f:AccessPolicy ; f:action ?action }`.
- **Versionable** — change policies through normal transactions; full history kept.
- **Branchable** — try new policies on a branch before merging to main.

## Best practices

1. **Start with `default-allow: false` and required policies.** Fail-closed is easier to reason about than fail-open.
2. **Tag every stored policy with a class** (e.g. `ex:CorpPolicy`) and tag every identity with `f:policyClass`. Pass `policy-class` at query time — Fluree pulls in the matching policy set automatically.
3. **Use `f:onProperty` / `f:onClass` / `f:onSubject` aggressively.** A targeted policy is cheaper to evaluate than a default policy, because Fluree can short-circuit during flake filtering.
4. **Keep `f:query` simple.** It runs once per flake-target during evaluation. Lean on tagged identity properties (`@type`, `f:policyClass`, role flags) rather than deep traversals.
5. **Test with multiple identities.** Verify the same query returns the right shape for each role.
6. **Document intent.** Add `rdfs:label` and `rdfs:comment` to your policy nodes so audits are readable.

## Related documentation

- [Policy enforcement (concepts)](../concepts/policy-enforcement.md) — model and architecture
- [Policy model and inputs](../security/policy-model.md) — full reference
- [Policy in queries](../security/policy-in-queries.md) — query-time enforcement details
- [Policy in transactions](../security/policy-in-transactions.md) — transaction-time enforcement
- [Programmatic policy API (Rust)](../security/programmatic-policy.md) — building policy contexts in code
- [Authentication](../security/authentication.md) — identity, JWTs, and bearer tokens
