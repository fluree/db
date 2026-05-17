# Policy Enforcement

Fluree enforces access control inside the database. Individual facts (flakes) are filtered against policy rules during query and transaction execution, so the same query returns different results to different identities — automatically. The application doesn't filter; the database does.

## Why triple-level

Most databases enforce access at the row, table, or schema level. That granularity is awkward for graph data, where a single subject may have facts that are public (`schema:name`), employee-only (`ex:department`), and HR-only (`ex:salary`). Fluree's enforcement happens **per flake** — `?subject ?predicate ?object` — so policies can permit `name`, allow `department` to platform employees, and restrict `salary` to managers in the same department, all from one query.

The consequences:

- **No application-side filtering.** Security can't be bypassed by buggy code paths because the database never returns flakes the requester isn't allowed to see.
- **Auditable.** Policies are themselves data. They live in the ledger, are time-travelable, and can be queried — `SELECT ?p WHERE { ?p a f:AccessPolicy }`.
- **Multi-tenant ready.** A single ledger can serve many tenants, with isolation enforced at flake level.
- **Compliance-friendly.** GDPR / HIPAA-style "minimum necessary" access is the default behavior, not a check the app forgot to do.

## What a policy looks like

Every policy is a JSON-LD node typed `f:AccessPolicy`. A policy has three orthogonal pieces:

- **Targeting** — `f:onProperty`, `f:onClass`, `f:onSubject` (each an array of `@id` references). Omit them all to make a *default policy* that applies to every flake.
- **Action** — `f:action` with values `f:view` (queries) and/or `f:modify` (transactions).
- **Decision** — either:
  - `f:allow: true` — unconditional allow, or
  - `f:allow: false` — unconditional deny, or
  - `f:query: "<JSON-encoded WHERE>"` — allow when the embedded query produces at least one binding for the targeted flake.

Two further knobs:

- `f:required: true` — the policy *must* allow for access to the targeted flake to be granted, even when `default-allow` is true. Use it for hard constraints (PII protection, write barriers).
- `f:exMessage` — a string returned to the caller when this policy denies a transaction.

A worked example:

```json
{
  "@id": "ex:salary-restriction",
  "@type": ["f:AccessPolicy", "ex:CorpPolicy"],
  "f:required": true,
  "f:onProperty": [{"@id": "ex:salary"}],
  "f:action": [{"@id": "f:view"}],
  "f:query": "{\"where\": {\"@id\": \"?$identity\", \"http://example.org/role\": \"manager\"}}"
}
```

Translation: *for every flake whose property is `ex:salary` and that someone is trying to read, this policy must allow. The embedded `f:query` runs with `?$identity` pre-bound to the requester; if it returns a binding (i.e. the identity has role `"manager"`), the flake is permitted.*

## Variables in `f:query`

Inside an `f:query`, two variables are pre-bound:

| Variable | Meaning |
|----------|---------|
| `?$this` | The subject of the targeted flake (the entity being read or written). |
| `?$identity` | The IRI of the requesting identity, supplied via `policy-values`. |

Anything else is bound by the embedded WHERE just like a normal Fluree query.

## How the engine combines policies

When a request hits a flake, the engine collects every policy that targets it:

1. **Required policies** (with `f:required: true`) must all allow. If any required policy denies — including by returning no `f:query` bindings — the flake is denied.
2. If no required policies target the flake, **any** allow is enough. Fluree uses *allow-overrides* across the non-required set.
3. If no policies apply at all, the request falls back to `default-allow`.

`default-allow: false` is fail-closed and the right choice for most production deployments.

## Where policies come from

Two delivery channels, often mixed:

- **Stored** — write policies into the ledger as data. Tag each policy with a class (e.g. `ex:CorpPolicy`), and tag each identity entity with `f:policyClass` linking to that class. At request time, pass `policy-class: ["ex:CorpPolicy"]` and the engine pulls the matching policy set from the ledger automatically. Stored policies are versioned, time-travelable, and consistent across all callers — the right approach for production.
- **Inline** — pass policies in `opts.policy` (an array of policy nodes) or via the `fluree-policy` HTTP header. Useful for ad-hoc queries, automated tests, and admin scripts.

The two can be combined: a query can carry a `policy-class` *and* an additional inline `policy`.

## Identity binding

An identity entity ties a caller (DID, JWT subject, application user) to graph nodes that policies can reason about:

```json
{
  "@id": "ex:aliceIdentity",
  "ex:user": {"@id": "ex:alice"},
  "f:policyClass": [{"@id": "ex:CorpPolicy"}]
}
```

Caller traffic carrying `identity: "ex:aliceIdentity"` causes:

1. Fluree binds `?$identity` to `ex:aliceIdentity` in every `f:query`.
2. Stored policies tagged `ex:CorpPolicy` are loaded.
3. Each policy's `f:query` runs against the snapshot, with `?$identity` and `?$this` pre-bound, deciding flake by flake whether the request is permitted.

The `ex:user` link is a domain-specific convention — your `f:query`s use it to reach from the identity to the human/service the policies should reason about. Any modeling works; nothing about that link is special to Fluree.

## What you control at the request boundary

Each request can supply:

- **`identity`** — IRI of the calling identity entity. Used to pre-bind `?$identity` and to discover the identity's `f:policyClass`.
- **`policy-class`** — one or more class IRIs to pull stored policies by class.
- **`policy-values`** — an object of additional `?$var` bindings injected into every policy's `f:query`.
- **`policy`** — an inline JSON-LD policy array.
- **`default-allow`** — boolean fallback for flakes no policy targets.

Over JSON-LD, these go inside `opts`. Over SPARQL, they're sent as `fluree-*` headers (SPARQL has no `opts` block). When the server is configured with a default policy class, a verified bearer token's identity is auto-applied — see the [policy cookbook](../guides/cookbook-policies.md#invoking-policies-via-http) for the request shapes and the server-side `data_auth_default_policy_class` option in [Configuration](../operations/configuration.md).

## Query enforcement vs transaction enforcement

The same policy model governs both, distinguished by `f:action`:

- **`f:view`** — runs during query execution. Flakes that fail the policy are filtered from the result; the query never sees them.
- **`f:modify`** — runs during transaction staging. The transaction is rejected (with `f:exMessage` if provided) if a write would touch flakes the identity isn't allowed to modify.

A single policy can govern both (`"f:action": [{"@id": "f:view"}, {"@id": "f:modify"}]`). Most realistic policy sets mix view-only restrictions, modify-only restrictions, and a small number of `[f:view, f:modify]` defaults.

## Policies are data

Because policies are flakes:

- **Time travel.** Query at past `t` to see what was in effect.
- **Branchable.** Trial policies on a branch before merging.
- **Versionable.** Edit through normal transactions; full history kept.
- **Self-querying.** Run reports over the policies themselves.

This makes policy management a normal Fluree workflow rather than a sidecar problem.

## Performance shape

Policy evaluation has two phases — load (read the policies relevant to this request once) and apply (filter flakes during plan execution). Cost scales mostly with the apply phase: how many flakes the request touches, and how expensive each policy's `f:query` is.

Two practical implications:

- **Target policies.** A policy with `f:onProperty` or `f:onClass` only runs on flakes whose predicate or rdf:type matches. Default policies (no targeting) run on every flake. Prefer targeting wherever it makes sense.
- **Keep `f:query` cheap.** Lean on identity attributes already loaded (`@type`, `f:policyClass`, role flags) rather than deep traversals.

For deeper architectural detail see [Policy model and inputs](../security/policy-model.md), [Policy in queries](../security/policy-in-queries.md), and [Policy in transactions](../security/policy-in-transactions.md).

## Related documentation

- [Cookbook: Access control policies](../guides/cookbook-policies.md) — worked examples for common patterns
- [Policy model and inputs](../security/policy-model.md) — full reference
- [Policy in queries](../security/policy-in-queries.md) — query-time behavior
- [Policy in transactions](../security/policy-in-transactions.md) — transaction-time behavior
- [Programmatic policy API (Rust)](../security/programmatic-policy.md) — building policy contexts in code
- [Cross-ledger policy](../security/cross-ledger-policy.md) — govern many data ledgers from one model ledger
- [Authentication](../security/authentication.md) — identity, JWTs, and bearer tokens
- [Configuration](../operations/configuration.md) — server-side policy defaults (`data_auth_default_policy_class`, etc.)
