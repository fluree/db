# Policy Model and Inputs

This is the reference for Fluree's access-control policy model. For a conceptual introduction, see [Policy enforcement](../concepts/policy-enforcement.md). For worked examples, see the [policy cookbook](../guides/cookbook-policies.md). For Rust-side wiring (building a `PolicyContext`, `wrap_identity_policy_view`, transaction helpers), see [Programmatic policy API](programmatic-policy.md).

## Policy node shape

Every policy is a JSON-LD node. Required `@type`: `f:AccessPolicy` (the IRI is `https://ns.flur.ee/db#AccessPolicy`). A second class IRI (e.g. `ex:CorpPolicy`) is conventional and allows the policy to be loaded by `policy-class`.

```json
{
  "@id": "ex:somePolicy",
  "@type": ["f:AccessPolicy", "ex:CorpPolicy"],
  "f:required": true,
  "f:onProperty": [{"@id": "ex:salary"}],
  "f:onClass":    [{"@id": "ex:Employee"}],
  "f:onSubject":  [{"@id": "ex:alice"}],
  "f:action": [{"@id": "f:view"}, {"@id": "f:modify"}],
  "f:query": "<JSON-encoded WHERE>",
  "f:allow": true,
  "f:exMessage": "Reason returned to caller on denial"
}
```

### Predicate reference

| Predicate | Type | Required? | Description |
|-----------|------|-----------|-------------|
| `f:action` | array of IRIs (or single IRI string) | yes | Which operations the policy governs. Values: `f:view` (queries), `f:modify` (transactions). |
| `f:allow` | boolean | one of `f:allow` / `f:query` | Static decision. `true` permits, `false` denies. Takes precedence over `f:query` if both are present. |
| `f:query` | string (JSON-encoded JSON-LD WHERE, or SPARQL with the `f:sparql` datatype) | one of `f:allow` / `f:query` | Dynamic decision. The targeted flake is permitted when the query returns at least one row. `?$this` and `?$identity` are pre-bound (`$this` / `$identity` in SPARQL). |
| `f:queryState` | IRI | no, defaults to `f:preState` | Which transaction state `f:query` evaluates against: `f:preState` (committed state before the transaction) or `f:postState` (committed + this transaction's staged flakes). See [Condition state](#condition-state-fquerystate). |
| `f:onProperty` | array of `@id` references | no | Restrict the policy to flakes whose predicate is one of these IRIs. |
| `f:onClass` | array of `@id` references | no | Restrict the policy to flakes whose subject has one of these `rdf:type`s. |
| `f:onSubject` | array of `@id` references | no | Restrict the policy to flakes whose subject IRI is one of these. |
| `f:required` | boolean | no, defaults to `false` | When `true`, the policy MUST allow for access to its targets to be granted, regardless of `default-allow`. |
| `f:exMessage` | string | no | User-facing error message returned when this policy denies a transaction. |

If neither `f:allow` nor `f:query` is present, the policy is **deny by default**.

If multiple targeting predicates are present, they intersect: the policy applies only to flakes that match the property AND the class AND the subject sets.

If all targeting predicates are omitted, the policy is a **default policy** that applies to every flake of its `f:action`s.

### Action values

`f:action` carries IRIs in the `f:` namespace:

- `"f:view"` (or `{"@id": "f:view"}`) — queries.
- `"f:modify"` (or `{"@id": "f:modify"}`) — transactions (all writes).
- Both: `[{"@id": "f:view"}, {"@id": "f:modify"}]`.
- Write verbs — `"f:create"`, `"f:update"`, `"f:delete"` — refine the
  transaction side by the subject's lifecycle within the transaction. See
  [Write verbs](policy-in-transactions.md#write-verbs) for full semantics.

A policy with no `f:action` defaults to applying to both view and modify.

**Verbs vs bare `f:modify`.** The write verbs select *exact* lifecycle
semantics: class targeting matches the subject's classes before **or**
after the transaction (so "may create new instances of `ex:Lead`" is
expressible), and `rdf:type` writes match by the class they assert or
retract. Bare `f:modify` keeps its long-standing semantics: it governs all
writes, with class targeting matched against the subject's pre-transaction
classes only (so class-targeted policies never apply to brand-new
subjects). Naming `f:modify` alongside a verb widens the verb set to all
three while keeping exact semantics.

## `f:query` syntax

A policy condition is an **existence check** — an ASK — in every language: the policy permits when the condition query has at least one solution.

For the JSON-LD form, `f:query` is a string containing a JSON-encoded query whose pattern is given under the `ask` key (preferred) or the legacy `where` key — the two are equivalent:

```json
"f:query": "{\"ask\": {\"@id\": \"?$identity\", \"http://example.org/role\": \"hr\"}}"
```

A condition carrying both `ask` and `where` is ambiguous and fails closed (deny). The engine runs the pattern as a subquery for each candidate flake, with two pre-bound variables:

| Variable | Binding |
|----------|---------|
| `?$this` | The IRI of the subject being read or written. |
| `?$identity` | The IRI of the requesting identity (resolved from `opts.identity`, `policy_values["?$identity"]`, or the verified bearer-token subject). |
| `?$value` | The object of the flake being authorized — an IRI ref or a literal. Lets a condition constrain the value being written ("stage may only be set to `qualified`"). |
| `?$op` | `"assert"` or `"retract"` (reads always bind `"assert"`). A value gate typically exempts retractions — a value change retracts the old value, whose `?$value` would otherwise fail the constraint: `FILTER($op = "retract" || $value = "qualified")`. |

Anything else binds via the embedded WHERE just like a normal Fluree query.
In SPARQL these bind as `$this` / `$identity` / `$value` / `$op`; in Cypher
they are supplied as the parameters `$this` / `$identity` / `$value` / `$op`.

Because RDF can't carry structured JSON values natively, stored policies must JSON-encode the query (`serde_json::to_string`). For inline policies passed via `opts.policy`, you can also use the JSON-LD typed-literal form `{"@type": "@json", "@value": {...}}` to avoid manually escaping.

Example (string form, suitable for storing in a transaction):

```json
"f:query": "{\"where\": {\"@id\": \"?$identity\", \"http://example.org/role\": \"hr\"}}"
```

Example (typed-literal form, suitable for inline policies):

```json
"f:query": {
  "@type": "@json",
  "@value": {
    "where": {"@id": "?$identity", "http://example.org/role": "hr"}
  }
}
```

> **Inline policies must use full IRIs.** Compact IRIs (`schema:ssn`) inside an inline policy passed through `opts.policy` are not expanded against the request `@context`. Use full IRIs (`http://schema.org/ssn`).

## SPARQL policy queries

`f:query` can alternatively be written in SPARQL. The language of the stored
literal is selected by its RDF datatype: `@json` (or a bare string) means
JSON-LD query; the `f:sparql` datatype (`https://ns.flur.ee/db#sparql`) means
SPARQL source text.

```json
"f:query": {
  "@type": "https://ns.flur.ee/db#sparql",
  "@value": "ASK { $identity <http://example.org/role> \"hr\" }"
}
```

The same typed-value form works for inline policies via `opts.policy` (the
compact `"@type": "f:sparql"` is also accepted there).

Rules for SPARQL policy queries:

- The query must be **ASK or SELECT** (both are evaluated as existence
  checks — the policy permits when at least one solution exists).
  CONSTRUCT/DESCRIBE/updates are rejected at policy build time and the
  policy falls back to **deny** (same fail-closed behavior as unparseable
  JSON).
- Special variables follow the SHACL-SPARQL convention: **`$this`** is the
  subject being read or written, **`$identity`** is the requesting identity,
  **`$value`** is the flake's object, **`$op`** is `"assert"` / `"retract"`.
  (`?this` / `?identity` are equivalent spellings; SPARQL variable names
  cannot contain `$`, so the JSON-LD names `?$this` / `?$identity` map to
  `$this` / `$identity`.)
- Include `PREFIX` declarations in the query text or use full IRIs — the
  request `@context` is not applied to policy sources.
- GROUP BY / aggregates are not supported in policy queries.

## Cypher policy queries

`f:query` can also be written in openCypher, selected by the `f:cypher`
datatype (`https://ns.flur.ee/db#cypher`):

```json
"f:query": {
  "@type": "https://ns.flur.ee/db#cypher",
  "@value": "MATCH (i)-[:`http://example.org/ns/user`]->(t) WHERE id(i) = $identity AND id(t) = $this RETURN i"
}
```

Rules for Cypher policy queries:

- The query must be **read-only** (MATCH...RETURN). Writes
  (CREATE/MERGE/SET/DELETE), schema commands, and procedure calls are
  rejected at policy build time and the policy falls back to **deny**.
- The condition holds when the query has **at least one result row** —
  the same existence semantics as every other condition language.
- **`$this`** and **`$identity`** are supplied as Cypher **parameters**
  carrying the subject / identity IRI strings — compare them with
  `id(n)` / `elementId(n)` or use them as property values. **`$value`**
  (the flake's object — an IRI string for refs, a scalar for literals) and
  **`$op`** (`"assert"` / `"retract"`) are also supplied. A request with
  no identity substitutes `$identity` as `null`, which never compares
  equal, so identity-referencing conditions cannot hold.
- Conditions lower without a ledger `@vocab`: write labels, relationship
  types, and property names as backtick-quoted full IRIs (as in the example
  above) unless the data itself was written without `@vocab`.
- Aggregates are not supported in policy queries.

The condition's language is independent of the request's protocol: a Cypher
condition governs JSON-LD and SPARQL reads and writes identically, because
enforcement operates on flakes.

## Condition state (`f:queryState`)

By default a condition evaluates against **pre-transaction state** — the
committed data before the write ("orders must currently be in draft",
owner checks against existing records). Declaring `f:queryState
f:postState` evaluates the whole condition against **post-transaction
state** (committed + this transaction's staged flakes) instead. That is
what "may create Leads owned by the requester" needs — the `ex:owner`
value is being asserted in the same transaction and is invisible to
pre-state:

```json
{
  "@id": "ex:leadCreatorsOwnOnly",
  "@type": "f:AccessPolicy",
  "f:onClass": [{"@id": "http://example.org/ns/Lead"}],
  "f:action": {"@id": "f:create"},
  "f:queryState": {"@id": "f:postState"},
  "f:query": {
    "@type": "f:sparql",
    "@value": "ASK { $this <http://example.org/ns/owner> $identity }"
  }
}
```

Notes:

- On the read path there is no transaction in flight, so `f:preState` and
  `f:postState` coincide with current state — a policy governing both
  actions stays portable.
- Many "transition" constraints don't need post-state at all: `?$value`
  carries the proposed value while the condition reads pre-state, so
  *"may set status to approved only from draft"* is
  `ASK { $this ex:status "draft" . FILTER($op = "retract" || $value = "approved") }`.
- The selector applies to the whole condition. Mixing both states inside
  one condition (relational transition constraints) is a planned SPARQL
  extension (`GRAPH f:postState { ... }`), not yet available.

## RDFS entailment

Policy targeting applies RDFS inference **always**: a policy with
`f:onClass ex:Employee` governs instances of every `rdfs:subClassOf*
ex:Employee` class, and a policy with `f:onProperty ex:phone` governs every
`rdfs:subPropertyOf* ex:phone` property. The hierarchy reflects committed
state (kept current automatically; no reindex needed). Cross-ledger policy
wires are not expanded (no hierarchy handle in that path yet).

## Combining algorithm

When more than one policy targets the same flake, the engine combines them as follows:

1. If any **required** policy (`f:required: true`) targets the flake and does not allow it (either `f:allow: false`, missing `f:allow`, or `f:query` returning no rows), access is **denied** for that flake. Required policies are *gates*: they cannot be overridden by other allows or by `default-allow`.
2. If at least one targeted (but not required) policy allows the flake, access is **granted**. Non-required allows combine with allow-overrides semantics.
3. If a targeted policy's `f:query` returns false (no rows), that policy *applied but did not permit* — the flake is denied even if `default-allow` is `true`. Default-allow only applies when **no** policy targets the flake.
4. If no policies target the flake, `default-allow` decides. `false` denies; `true` permits.

`f:allow` always takes precedence over `f:query`: if both are set on the same policy, `f:allow` wins.

For a deeper treatment, including the three-state identity resolution semantics (`FoundWithPolicies` / `FoundNoPolicies` / `NotFound`), see the [Policy combining algorithm](programmatic-policy.md#policy-combining-algorithm) section in the programmatic policy API reference.

## Default-allow

`default-allow` is the fallback decision for flakes that no policy targets:

| Setting | Behavior |
|---------|----------|
| `default-allow: false` | Fail-closed. A flake with no targeting policies is denied. **Recommended for production.** |
| `default-allow: true` | Fail-open. A flake with no targeting policies is allowed. Useful in development or in deployments where an application layer handles authorization and Fluree is recording signed transactions for provenance. |

Important: `default-allow: true` does **not** override required policies that fail. It only governs the no-policy case.

## Identity resolution

When `opts.identity` is set, Fluree resolves it to a `?$identity` SID and applies the identity's `f:policyClass` automatically — every stored policy of that class is loaded into the request's policy set.

The resolution path:

```
opts.identity  →  policy_class               →  policy             →  policy_values["?$identity"]
   (highest)                                                                  (lowest)
```

If multiple are set, the higher-priority binding wins. `policy_values["?$identity"]` is a manual escape hatch — useful when you want to test a specific identity SID without going through the full resolution path.

A request with no identity supplied uses an "anonymous" context: only inline policies, no class-based discovery, no `?$identity` binding.

## Where policies come from

Two delivery paths, often combined:

### Stored policies

Persist policies as data in the ledger. The policy node carries the class type alongside `f:AccessPolicy`:

```json
{
  "@id": "ex:salary-restriction",
  "@type": ["f:AccessPolicy", "ex:CorpPolicy"],
  ...
}
```

Identities tag themselves with `f:policyClass`:

```json
{
  "@id": "ex:aliceIdentity",
  "ex:user": {"@id": "ex:alice"},
  "f:policyClass": [{"@id": "ex:CorpPolicy"}]
}
```

When `opts.identity = "ex:aliceIdentity"`, every `f:AccessPolicy` whose `@type` includes `ex:CorpPolicy` is loaded for the request — no per-request policy listing needed. Stored policies are versioned, time-travelable, branchable, and consistent across all callers.

### Inline policies

Pass policies in `opts.policy` (an array of policy nodes) for ad-hoc requests:

```json
{
  "from": "mydb:main",
  "select": "?x",
  "where": [...],
  "opts": {
    "policy": [
      {"@id": "ex:adhoc", "@type": "f:AccessPolicy", "f:action": "f:view", "f:allow": true}
    ],
    "default-allow": false
  }
}
```

Useful for tests, admin scripts, and migration tooling. Inline policies and stored policies can coexist in a single request: inline policies always merge **on top of** whatever stored policies the request selects — via `identity`, `policy-class`, or a cross-ledger `f:policySource` — never instead of them.

## Request-time options

Each request can supply these `opts` fields (JSON-LD form). Over SPARQL, the equivalent fluree-* HTTP headers carry the same values.

| `opts` field | HTTP header | Description |
|--------------|-------------|-------------|
| `identity` | `fluree-identity` | IRI of an identity entity. Drives `f:policyClass` discovery and binds `?$identity`. |
| `policy-class` | `fluree-policy-class` | Class IRI(s) to load stored policies by. Repeated header or comma-separated. |
| `policy-values` | `fluree-policy-values` | JSON object of additional `?$var` bindings injected into every policy's `f:query`. |
| `policy` | `fluree-policy` | Inline policy array (full JSON-LD). |
| `default-allow` | `fluree-default-allow` | `true` / `false`. Fallback decision for flakes that no policy targets. |

When the server is configured with `data_auth_default_policy_class`, a verified bearer token's identity claim is auto-applied to `policy-values` and the configured class to `policy-class` — no client-side opts needed. See [Configuration](../operations/configuration.md) and [Authentication](authentication.md) for the bearer-token flow.

## Read enforcement vs write enforcement

The same model governs both, distinguished by `f:action`:

- **`f:view`** — applied during query execution. Flakes that fail the policy are filtered before the query plan emits results. The query never sees them.
- **`f:modify`** — applied during transaction staging. The transaction is rejected — with `f:exMessage` if provided — when a write would touch flakes the identity isn't allowed to modify.

A single policy can govern both. See [Policy in queries](policy-in-queries.md) and [Policy in transactions](policy-in-transactions.md) for path-specific details.

## Performance notes

Two phases:

- **Load.** The relevant policies for a request are gathered once (from `policy-class` lookups + inline `policy`). Cost is small and proportional to the size of the policy set.
- **Apply.** During plan execution, each candidate flake is checked against the matching subset of the policy set. Cost is proportional to the number of touched flakes × the average per-flake check cost.

Two practical implications:

1. **Target every policy you can.** A policy with `f:onProperty` or `f:onClass` only runs on flakes whose predicate or rdf:type matches. Default policies (no targeting) run on every flake.
2. **Keep `f:query` cheap.** It runs once per targeted flake. Lean on identity-side properties already loaded (`@type`, `f:policyClass`, role flags) rather than deep traversals.

## Policies are queryable data

Because each policy is just a JSON-LD node, you can query the policies themselves:

```sparql
PREFIX f: <https://ns.flur.ee/db#>
PREFIX ex: <http://example.org/>

SELECT ?policy ?action ?onProperty
WHERE {
  ?policy a f:AccessPolicy ;
          a ex:CorpPolicy ;
          f:action ?action ;
          f:onProperty ?onProperty .
}
```

History queries against the same shape produce a complete audit trail of policy changes over time. See [Time travel](../concepts/time-travel.md) for query-at-t syntax.

## Related documentation

- [Policy enforcement (concepts)](../concepts/policy-enforcement.md) — model and architecture
- [Cookbook: Access control policies](../guides/cookbook-policies.md) — worked examples and patterns
- [Policy in queries](policy-in-queries.md) — read-time enforcement details
- [Policy in transactions](policy-in-transactions.md) — write-time enforcement details
- [Programmatic policy API (Rust)](programmatic-policy.md) — `PolicyContext`, builder helpers, combining algorithm
- [Cross-ledger policy](cross-ledger-policy.md) — one model ledger governs many data ledgers via `f:policySource` with `f:ledger`
- [Authentication](authentication.md) — identities, JWTs, bearer-token verification
- [Configuration](../operations/configuration.md) — server-side policy defaults (`data_auth_default_policy_class`, etc.)
- [Vocabulary reference](../reference/vocabulary.md#policy-vocabulary) — predicate IRIs
