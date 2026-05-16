# Ledger Configuration (Config Graph)

Fluree stores **ledger-level configuration as data** inside each ledger, in a dedicated system graph called the **config graph**. This is distinct from [server configuration](../operations/configuration.md) (TOML files, environment variables) which controls how the Fluree process runs.

The config graph holds RDF triples that define operational defaults for the ledger: which policy rules apply, whether SHACL validation runs, what reasoning modes are active, which properties enforce uniqueness, and more. Because config lives inside the ledger, it is:

- **Immutable and time-travelable** — config at any historical `t` is recoverable
- **Auditable** — every config change is a signed, committed transaction
- **Replicable** — config travels with the ledger across nodes and forks
- **Replay-safe** — deterministic interpretation without runtime environment state

## Graph layout

Every ledger reserves system named graphs:

| Graph | IRI pattern | Purpose |
|-------|-------------|---------|
| Default graph | (implicit) | Application data |
| Txn-meta | `urn:fluree:{ledger_id}#txn-meta` | Commit metadata |
| Config graph | `urn:fluree:{ledger_id}#config` | Ledger configuration |

User-defined named graphs (created via TriG) are identified by their IRI and allocated after the system graphs.

The config graph IRI is deterministic — derived from the ledger identifier. For a ledger `mydb:main`, the config graph is `urn:fluree:mydb:main#config`.

## Core concepts

### `f:LedgerConfig`

A single `f:LedgerConfig` resource in the config graph defines ledger-wide defaults. If multiple exist, the one with the lexicographically smallest `@id` wins (with a logged warning).

### Setting groups

Configuration is organized into independent **setting groups**, each governing a different subsystem:

| Setting group | Subsystem | Key fields |
|---------------|-----------|------------|
| [`f:policyDefaults`](setting-groups.md#policy-defaults) | Policy enforcement | `f:defaultAllow`, `f:policySource`, `f:policyClass` |
| [`f:shaclDefaults`](setting-groups.md#shacl-defaults) | SHACL validation | `f:shaclEnabled`, `f:shapesSource`, `f:validationMode` |
| [`f:reasoningDefaults`](setting-groups.md#reasoning-defaults) | OWL/RDFS reasoning | `f:reasoningModes`, `f:schemaSource` |
| [`f:datalogDefaults`](setting-groups.md#datalog-defaults) | Datalog rules | `f:datalogEnabled`, `f:rulesSource` |
| [`f:transactDefaults`](setting-groups.md#transact-defaults) | Transaction constraints | `f:uniqueEnabled`, `f:constraintsSource` |

Each group is resolved independently — locking down policy does not affect whether reasoning can be overridden.

### Per-graph overrides

Ledger-wide defaults apply to all graphs. For finer control, `f:graphOverrides` on the `f:LedgerConfig` contains `f:GraphConfig` entries that override settings for specific named graphs. See [Override control](override-control.md) for the full resolution model.

### Privileged system read

Config is read via a **privileged system read** that bypasses policy enforcement. This is necessary because config defines the policy — reading it through the policy-enforced path would create a circular dependency. User queries against the config graph still go through normal policy enforcement.

### Lagging config

Config changes take effect on the **next** transaction, not the current one. The transaction pipeline reads config from the pre-transaction state. This prevents a transaction from "authorizing itself" by changing config within its own payload.

## Common patterns

These recipes cover typical scenarios. Each assumes the ledger `mydb:main` — substitute your own ledger ID.

### Lock down a production ledger

Deny all access by default and require policy rules for every operation. Use `f:OverrideNone` so no query can bypass:

```trig
@prefix f: <https://ns.flur.ee/db#> .

GRAPH <urn:fluree:mydb:main#config> {
  <urn:fluree:mydb:main:config:ledger> a f:LedgerConfig ;
    f:policyDefaults [
      f:defaultAllow false ;
      f:policySource [
        a f:GraphRef ;
        f:graphSource [ f:graphSelector f:defaultGraph ]
      ] ;
      f:overrideControl f:OverrideNone
    ] .
}
```

After this transaction, the **next** transaction and all subsequent queries will require matching policy rules in the default graph. Make sure policy rules are already in place before enabling this — see [Config mutation governance](writing-config.md#config-mutation-governance).

### Enable SHACL validation in development (warn mode)

Validate data shapes but log warnings instead of rejecting — useful during development:

```trig
@prefix f: <https://ns.flur.ee/db#> .

GRAPH <urn:fluree:mydb:main#config> {
  <urn:fluree:mydb:main:config:ledger> a f:LedgerConfig ;
    f:shaclDefaults [
      f:shaclEnabled true ;
      f:shapesSource [
        a f:GraphRef ;
        f:graphSource [ f:graphSelector f:defaultGraph ]
      ] ;
      f:validationMode f:ValidationWarn
    ] .
}
```

Switch to `f:ValidationReject` when ready for production.

### Enforce unique emails

Two-step setup: annotate the property, then enable enforcement:

```trig
@prefix f:  <https://ns.flur.ee/db#> .
@prefix ex: <http://example.org/ns/> .

# Step 1: Annotate the property (in the default graph)
ex:email f:enforceUnique true .

# Step 2: Enable enforcement (in the config graph)
GRAPH <urn:fluree:mydb:main#config> {
  <urn:fluree:mydb:main:config:ledger> a f:LedgerConfig ;
    f:transactDefaults [
      f:uniqueEnabled true
    ] .
}
```

See [Unique constraints](unique-constraints.md) for full details including per-graph scoping and edge cases.

### Enable RDFS reasoning by default

Automatically expand `rdfs:subClassOf` and `rdfs:subPropertyOf` in all queries:

```trig
@prefix f: <https://ns.flur.ee/db#> .

GRAPH <urn:fluree:mydb:main#config> {
  <urn:fluree:mydb:main:config:ledger> a f:LedgerConfig ;
    f:reasoningDefaults [
      f:reasoningModes f:RDFS ;
      f:schemaSource [
        a f:GraphRef ;
        f:graphSource [ f:graphSelector f:defaultGraph ]
      ]
    ] .
}
```

With `f:OverrideAll` (the default), individual queries can still opt out by passing `"reasoning": "none"`.

### Different policy per graph

Allow open access to most graphs but lock down a sensitive one:

```trig
@prefix f: <https://ns.flur.ee/db#> .

GRAPH <urn:fluree:mydb:main#config> {
  <urn:fluree:mydb:main:config:ledger> a f:LedgerConfig ;
    f:policyDefaults [
      f:defaultAllow true ;
      f:overrideControl f:OverrideAll
    ] ;
    f:graphOverrides (
      [ a f:GraphConfig ;
        f:targetGraph <http://example.org/sensitive> ;
        f:policyDefaults [
          f:defaultAllow false ;
          f:policySource [
            a f:GraphRef ;
            f:graphSource [ f:graphSelector f:defaultGraph ]
          ] ;
          f:overrideControl f:OverrideNone
        ]
      ]
    ) .
}
```

The sensitive graph requires policy rules and cannot be overridden at query time. All other graphs remain open.

## Troubleshooting

### Config changes seem to have no effect

Config uses **lagging semantics** — changes take effect on the **next** transaction, not the current one. If you enable SHACL and insert invalid data in the same transaction, the data will be accepted. The next transaction will enforce the new config.

### Ledger became unmodifiable after policy misconfiguration

If you set `f:defaultAllow false` with `f:OverrideNone` before granting write access to the config graph, the ledger becomes locked — no transaction can modify it (including config changes). Recovery requires a ledger fork/restore. To prevent this:

1. **Always write policy rules first**, then enable restrictive policy in a subsequent transaction
2. **Test with `f:OverrideAll`** before switching to `f:OverrideNone`
3. **Ensure at least one identity** has write access to the config graph before locking down

### Multiple `f:LedgerConfig` resources

If the config graph contains more than one `f:LedgerConfig` resource, the system uses the one with the **lexicographically smallest `@id`** and logs a warning. Use the recommended subject IRI convention (`urn:fluree:{ledger_id}:config:ledger`) to avoid this.

### Config graph query returns empty results

User queries against the config graph go through **policy enforcement**. If `f:defaultAllow` is `false` and no policy explicitly grants read access to the config graph, queries will return empty results even though config is active. The system's internal privileged read is unaffected.

## CLI usage

The config graph is written and queried through normal CLI transaction and query commands:

```bash
# Write config via TriG
fluree insert --ledger mydb:main --format trig config.trig

# Query the config graph via SPARQL
fluree query --ledger mydb:main --format sparql \
  'PREFIX f: <https://ns.flur.ee/db#>
   SELECT ?s ?p ?o
   FROM <urn:fluree:mydb:main#config>
   WHERE { ?s ?p ?o }'
```

No special CLI commands are needed — config is data, written and queried like any other named graph.

## In this section

- [Writing config data](writing-config.md) — How to create and update config via TriG, SPARQL, or JSON-LD
- [Setting groups](setting-groups.md) — All setting groups with fields and examples
- [Override control](override-control.md) — Resolution precedence, identity gating, monotonicity
- [Unique constraints](unique-constraints.md) — Enforcing property value uniqueness with `f:enforceUnique`

## Related

- [Cross-ledger policy](../security/cross-ledger-policy.md) — Configure one model ledger to govern many data ledgers via `f:policySource` with `f:ledger`. Builds on the `f:GraphRef` shape documented in [setting groups](setting-groups.md#policy-defaults).
