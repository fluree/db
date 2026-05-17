# Policy in Queries

Query-time enforcement uses Fluree's [policy model](policy-model.md) to filter individual flakes during query execution. The query plan is the same regardless of policy — what changes is which flakes the engine returns. The application sees a query result; the policy filtering is invisible.

This page documents how query-time enforcement works, how patterns interact with the plan, and how to test policies from the CLI. For the policy node shape and combining algorithm, see the [policy model reference](policy-model.md). For the underlying concept, see [Policy enforcement](../concepts/policy-enforcement.md).

## How query-time filtering works

When a query is executed against a `PolicyContext`:

1. The engine resolves the request's policy set: identity-driven `f:policyClass` lookups + any inline `opts.policy` array.
2. The plan executes normally — same join order, same indices.
3. Each flake the plan would emit is checked against the policies whose target matches it (`f:onProperty`, `f:onClass`, `f:onSubject`, or default for untargeted policies).
4. A flake survives only if the [combining algorithm](policy-model.md#combining-algorithm) approves it.
5. Surviving flakes flow through the rest of the plan (joins, filters, aggregates) as normal.

Filtering is at the flake level — a single subject can appear in the result with some properties visible and others elided.

## Worked example

Two users in a `mydb:main` ledger:

```bash
fluree insert '{
  "@context": {"schema": "http://schema.org/", "ex": "http://example.org/"},
  "@graph": [
    {"@id": "ex:alice", "schema:name": "Alice", "ex:role": "engineer", "ex:salary": 130000},
    {"@id": "ex:bob",   "schema:name": "Bob",   "ex:role": "manager",  "ex:salary": 155000}
  ]
}'
```

A required policy that hides `ex:salary` unless the requester is a manager:

```bash
fluree insert '{
  "@context": {"f": "https://ns.flur.ee/db#", "ex": "http://example.org/"},
  "@graph": [
    {
      "@id": "ex:salary-restriction",
      "@type": ["f:AccessPolicy", "ex:CorpPolicy"],
      "f:required": true,
      "f:onProperty": [{"@id": "ex:salary"}],
      "f:action": [{"@id": "f:view"}],
      "f:query": "{\"where\": {\"@id\": \"?$identity\", \"http://example.org/role\": \"manager\"}}"
    },
    {
      "@id": "ex:default-view",
      "@type": ["f:AccessPolicy", "ex:CorpPolicy"],
      "f:action": [{"@id": "f:view"}],
      "f:allow": true
    },
    {"@id": "ex:aliceIdentity", "f:policyClass": [{"@id": "ex:CorpPolicy"}], "ex:role": "engineer"},
    {"@id": "ex:bobIdentity",   "f:policyClass": [{"@id": "ex:CorpPolicy"}], "ex:role": "manager"}
  ]
}'
```

The same query, executed as different identities:

```bash
# As Bob (manager) — sees salaries
fluree query --as ex:bobIdentity --policy-class ex:CorpPolicy \
  'SELECT ?name ?salary WHERE { ?p <http://schema.org/name> ?name ; <http://example.org/salary> ?salary }'
# → Alice 130000, Bob 155000

# As Alice (engineer) — salary flakes filtered out
fluree query --as ex:aliceIdentity --policy-class ex:CorpPolicy \
  'SELECT ?name ?salary WHERE { ?p <http://schema.org/name> ?name ; <http://example.org/salary> ?salary }'
# → no results: the join requires ?salary which is filtered for Alice
```

To get Alice's name back without the salary join, use `OPTIONAL`:

```sparql
SELECT ?name ?salary WHERE {
  ?p <http://schema.org/name> ?name .
  OPTIONAL { ?p <http://example.org/salary> ?salary }
}
```

Now Alice sees both names, with `?salary` unbound — exactly the behavior an application expects when a property is suppressed by policy.

## Targeting patterns

### Property-level (`f:onProperty`)

Restricts a flake whose predicate matches:

```json
{
  "@id": "ex:hide-ssn",
  "@type": ["f:AccessPolicy", "ex:CorpPolicy"],
  "f:required": true,
  "f:onProperty": [{"@id": "http://schema.org/ssn"}],
  "f:action": [{"@id": "f:view"}],
  "f:query": "{\"where\": {\"@id\": \"?$identity\", \"http://example.org/role\": \"hr\"}}"
}
```

Flakes whose predicate is not `schema:ssn` are unaffected by this policy.

### Class-level (`f:onClass`)

Restricts flakes whose subject has one of the listed `rdf:type`s:

```json
{
  "@id": "ex:employee-data-only",
  "@type": ["f:AccessPolicy", "ex:CorpPolicy"],
  "f:required": true,
  "f:onClass": [{"@id": "http://example.org/Employee"}],
  "f:action": [{"@id": "f:view"}],
  "f:query": "{\"where\": {\"@id\": \"?$identity\", \"@type\": \"http://example.org/Employee\"}}"
}
```

Flakes about non-`Employee` subjects fall through to other policies.

### Subject-level (`f:onSubject`)

Restricts flakes about specific subjects:

```json
{
  "@id": "ex:hide-internal-doc",
  "@type": ["f:AccessPolicy", "ex:CorpPolicy"],
  "f:required": true,
  "f:onSubject": [{"@id": "http://example.org/secret-doc"}],
  "f:action": [{"@id": "f:view"}],
  "f:allow": false
}
```

### Default (no targeting)

A policy with no `f:onProperty` / `f:onClass` / `f:onSubject` applies to **every** flake. Use sparingly — default policies are evaluated against every emitted flake, which is more expensive than targeted policies.

## SPARQL queries

SPARQL queries have no `opts` block, so policy is delivered via headers:

```bash
curl -X POST 'http://localhost:8090/v1/fluree/query?ledger=mydb:main' \
  -H 'Content-Type: application/sparql-query' \
  -H "Authorization: Bearer $JWT" \
  -H 'fluree-identity: ex:aliceIdentity' \
  -H 'fluree-policy-class: ex:CorpPolicy' \
  -H 'fluree-default-allow: false' \
  -d 'SELECT ?name WHERE { ?p <http://schema.org/name> ?name }'
```

The full header set is documented in the [policy model](policy-model.md#request-time-options).

## JSON-LD queries

JSON-LD queries put policy in `opts`:

```json
{
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
}
```

Inline policies, additional `policy-values`, and multiple `policy-class` entries all live under `opts`. The full vocabulary is in the [policy model reference](policy-model.md#request-time-options).

## Multi-graph queries

Policies apply per-flake, regardless of which named graph the flake came from. A query that pulls from multiple `from-named` graphs sees a uniformly filtered result — there's no per-graph policy override.

If different graphs need different policy regimes, use targeted policies (`f:onClass` for type-scoped restrictions, `f:onSubject` for explicit subject lists). For wholly separate access regimes, use separate ledgers.

## Time-travel queries

Policy evaluation honors the query's `t`. When you query `--at` a past `t`:

- The policy set itself is resolved at that `t` (so retired policies still apply when you time-travel back to when they were live).
- Identity attributes used in `f:query` are evaluated at that `t`.

This makes audit-style queries — *"What could Alice see on 2024-06-15?"* — directly expressible:

```bash
fluree query --as ex:aliceIdentity --policy-class ex:CorpPolicy --at 2024-06-15T00:00:00Z \
  'SELECT ?p ?o WHERE { <http://example.org/financial-report> ?p ?o }'
```

## Performance considerations

Two phases: load the policy set once per request; apply it to each touched flake.

- **Target policies whenever possible.** A policy with `f:onProperty` only runs against flakes whose predicate matches. Default policies (no targeting) run against every flake.
- **Keep `f:query` cheap.** It runs once per flake-target. Lean on identity-side properties already loaded (`@type`, `f:policyClass`, role flags) rather than deep traversals.
- **Avoid deep recursion in `f:query`.** Each level of indirection multiplies the per-flake cost.
- **Required policies short-circuit.** If a required policy denies, no further required policies are checked for that flake.

For complex deployments, the [explain plan](../query/explain.md) shows whether a query is dominated by policy filtering and which policies contribute.

## Testing policies from the CLI

The `fluree` CLI supports policy-enforced queries so you can verify that the policies you've configured filter results as expected — without writing any client code.

### Flags

Available on `fluree query` (and on `fluree insert`, `upsert`, `update` for write-time enforcement):

| Flag | Purpose |
|------|---------|
| `--as <IRI>` | Execute as this identity. Resolves `f:policyClass` on the identity subject to collect applicable policies, and binds `?$identity`. |
| `--policy-class <IRI>` | Apply stored policies of the given class IRI. Repeatable. Narrows to the intersection with the identity's policies, or applies directly without `--as`. |
| `--default-allow` | Allow when no matching policy exists for the operation. Defaults to `false` (deny-by-default). |

### Workflow

1. Transact your policy rules (and the identities with their `f:policyClass` assignments) into the ledger, using any of the normal insert / upsert / update commands.
2. Re-run the same query as different identities to confirm results differ as the policies prescribe:

```bash
# Full result set (no policy enforcement)
fluree query 'SELECT ?name ?salary WHERE { ?p <http://schema.org/name> ?name ; <http://example.org/salary> ?salary }'

# As an HR user — should see all salaries
fluree query --as ex:hrIdentity --policy-class ex:CorpPolicy \
  'SELECT ?name ?salary WHERE { ?p <http://schema.org/name> ?name ; <http://example.org/salary> ?salary }'

# As a regular employee — policies should hide salary field
fluree query --as ex:engineerIdentity --policy-class ex:CorpPolicy \
  'SELECT ?name ?salary WHERE { ?p <http://schema.org/name> ?name ; <http://example.org/salary> ?salary }'
```

### Local vs remote

The flags work in both modes:

- **Local** (default, or with `--direct`): the CLI loads the ledger directly and applies policy via the in-process query engine.
- **Remote** (with `--remote <name>`, or auto-routed through a running local server): the CLI sends the flags to the server as HTTP headers (`fluree-identity`, `fluree-policy-class`, `fluree-default-allow`) and, for JSON-LD bodies, also injects them into `opts`. Multi-value `--policy-class` rides through the body opts only; SPARQL transport is single-valued via the header.

### Remote impersonation: how it's authorized

When you run against a remote server with `--as <iri>`, the server treats the request as **impersonation** and gates it as follows:

1. Your bearer token's identity is resolved on the target ledger.
2. If that identity has **no** `f:policyClass` assignments (the `FoundNoPolicies` outcome — your service account is unrestricted on this ledger), the server honors `--as` and runs the query as the target identity.
3. If your bearer identity is itself policy-constrained (`FoundWithPolicies`) or unknown to this ledger (`NotFound`), the server force-overrides `--as` with your bearer identity. You see your own filtered view, not the target's.

Each successful impersonation is logged at `info` level on the server:

```
policy impersonation: bearer=<svc-id> target=<as-iri> ledger=<name>
```

This is the standard service-account pattern: register your CLI/app-server identity in the ledger with no `f:policyClass`, and it gains the right to delegate to any end-user identity for testing or per-request enforcement. Assigning a policy class to that identity revokes the delegation right with no config change.

### Limitations

- Inline policy rules (`opts.policy`) and policy variable bindings (`opts.policy-values`) are not yet exposed as CLI flags — use a JSON-LD query body with an `"opts"` block when you need those.
- For SPARQL queries against a remote, only `--as`, single-value `--policy-class`, and `--default-allow` are wired (via headers). Multi-value `--policy-class` works on JSON-LD only.
- Proxy-mode servers fall back to the legacy non-impersonation behavior — the upstream server performs the impersonation check.

## Related documentation

- [Policy model and inputs](policy-model.md) — node shape, combining algorithm, request-time options
- [Policy enforcement (concepts)](../concepts/policy-enforcement.md) — model overview
- [Policy in transactions](policy-in-transactions.md) — write-time enforcement
- [Cross-ledger policy](cross-ledger-policy.md) — query-time engagement under cross-ledger `f:policySource`
- [Cookbook: Access control policies](../guides/cookbook-policies.md) — worked patterns
- [Programmatic policy API (Rust)](programmatic-policy.md) — building `PolicyContext` in code
- [Query reference](../query/README.md) — SPARQL and JSON-LD syntax
- [Explain plans](../query/explain.md) — diagnosing policy filter overhead
